package com.baidu.fsg.uid.worker;

import com.baidu.fsg.uid.utils.DockerUtils;
import com.baidu.fsg.uid.utils.NetUtils;
import com.baidu.fsg.uid.worker.dao.WorkerNodeDAO;
import com.baidu.fsg.uid.worker.entity.WorkerNodeEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.Resource;

/**
 * Represents an implementation of {@link WorkerIdAssigner},
 * the worker id will be reused after assigned to the UidGenerator
 */
public class HostPortWorkerIdAssigner implements WorkerIdAssigner{
    private static final Logger LOGGER = LoggerFactory.getLogger(HostPortWorkerIdAssigner.class);

    @Resource
    private WorkerNodeDAO workerNodeDAO;
    @Value("${server.port:0}")
    private String port;

    /**
     * Assign worker id base on database.<p>
     * If there is host name & port in the environment, we considered that the node runs in Docker container<br>
     * Otherwise, the node runs on an actual machine.
     *
     * @return assigned worker id
     */
    @Override
    public long assignWorkerId() {
        // reuse worker id
        WorkerNodeEntity workerNodeEntity = workerNodeDAO.getWorkerNodeByHostPort(getHost(), port);
        if (workerNodeEntity != null) {
            LOGGER.info("Get worker id from db: {}", workerNodeEntity);
            return workerNodeEntity.getId();
        }

        // build worker node entity
        workerNodeEntity = buildWorkerNode();

        // add worker node for new
        workerNodeDAO.addWorkerNode(workerNodeEntity);
        LOGGER.info("Add worker node: {}", workerNodeEntity);

        return workerNodeEntity.getId();
    }

    private String getHost() {
        return DockerUtils.isDocker() ? DockerUtils.getDockerHost() : NetUtils.getLocalAddress();
    }

    /**
     * Build worker node entity by IP and PORT
     */
    private WorkerNodeEntity buildWorkerNode() {
        WorkerNodeEntity workerNodeEntity = new WorkerNodeEntity();
        if (DockerUtils.isDocker()) {
            workerNodeEntity.setType(WorkerNodeType.CONTAINER.value());
            workerNodeEntity.setHostName(DockerUtils.getDockerHost());
            workerNodeEntity.setPort(port);
        } else {
            workerNodeEntity.setType(WorkerNodeType.ACTUAL.value());
            workerNodeEntity.setHostName(NetUtils.getLocalAddress());
            workerNodeEntity.setPort(port);
        }

        return workerNodeEntity;
    }
}
