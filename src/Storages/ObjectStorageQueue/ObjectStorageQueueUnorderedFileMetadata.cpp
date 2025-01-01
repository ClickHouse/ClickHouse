#include <Storages/ObjectStorageQueue/ObjectStorageQueueUnorderedFileMetadata.h>
#include <Common/getRandomASCIIString.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    zkutil::ZooKeeperPtr getZooKeeper()
    {
        return Context::getGlobalContextInstance()->getZooKeeper();
    }
}

ObjectStorageQueueUnorderedFileMetadata::ObjectStorageQueueUnorderedFileMetadata(
    const std::filesystem::path & zk_path,
    const std::string & path_,
    FileStatusPtr file_status_,
    size_t max_loading_retries_,
    LoggerPtr log_)
    : ObjectStorageQueueIFileMetadata(
        path_,
        /* processing_node_path */zk_path / "processing" / getNodeName(path_),
        /* processed_node_path */zk_path / "processed" / getNodeName(path_),
        /* failed_node_path */zk_path / "failed" / getNodeName(path_),
        file_status_,
        max_loading_retries_,
        log_)
{
}

std::pair<bool, ObjectStorageQueueIFileMetadata::FileStatus::State> ObjectStorageQueueUnorderedFileMetadata::setProcessingImpl()
{
    const auto zk_client = getZooKeeper();
    processing_id = node_metadata.processing_id = getRandomASCIIString(10);
    auto processor_info = getProcessorInfo(processing_id.value());

    while (true)
    {
        Coordination::Requests requests;
        const size_t processed_path_doesnt_exist_idx = 0;
        zkutil::addCheckNotExistsRequest(requests, *zk_client, processed_node_path);
        const size_t failed_path_doesnt_exist_idx = requests.size();
        zkutil::addCheckNotExistsRequest(requests, *zk_client, failed_node_path);


        const auto created_processing_path_idx = requests.size();
        requests.push_back(zkutil::makeCreateRequest(processing_node_path, node_metadata.toString(), zkutil::CreateMode::Ephemeral));

        bool create_if_not_exists_enabled = zk_client->isFeatureEnabled(DB::KeeperFeatureFlag::CREATE_IF_NOT_EXISTS);
        if (create_if_not_exists_enabled)
        {
            requests.push_back(
                zkutil::makeCreateRequest(processing_node_id_path, "", zkutil::CreateMode::Persistent, /* ignore_if_exists */ true));
        }
        else if (!zk_client->exists(processing_node_id_path))
        {
            requests.push_back(zkutil::makeCreateRequest(processing_node_id_path, processor_info, zkutil::CreateMode::Persistent));
        }

        requests.push_back(zkutil::makeSetRequest(processing_node_id_path, processor_info, -1));

        const auto set_processing_id_idx = requests.size() - 1;

        Coordination::Responses responses;
        const auto code = zk_client->tryMulti(requests, responses);
        auto has_request_failed = [&](size_t request_index) { return responses[request_index]->error != Coordination::Error::ZOK; };

        if (code == Coordination::Error::ZOK)
        {
            const auto & set_response = dynamic_cast<const Coordination::SetResponse &>(*responses[set_processing_id_idx].get());
            processing_id_version = set_response.stat.version;
            return std::pair{true, FileStatus::State::None};
        }

        if (has_request_failed(processed_path_doesnt_exist_idx))
            return {false, FileStatus::State::Processed};

        if (has_request_failed(failed_path_doesnt_exist_idx))
            return {false, FileStatus::State::Failed};

        if (has_request_failed(created_processing_path_idx))
            return {false, FileStatus::State::Processing};

        if (create_if_not_exists_enabled)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected state of zookeeper transaction: {}", code);

        /// most likely the node was removed so let's try again
        LOG_TRACE(log, "Retrying setProcessing because processing node id path is unexpectedly missing or was created (error code: {})", code);
    }
}

void ObjectStorageQueueUnorderedFileMetadata::setProcessedAtStartRequests(
    Coordination::Requests & requests,
    const zkutil::ZooKeeperPtr &)
{
    requests.push_back(
        zkutil::makeCreateRequest(
            processed_node_path, node_metadata.toString(), zkutil::CreateMode::Persistent));
}

void ObjectStorageQueueUnorderedFileMetadata::setProcessedImpl()
{
    setProcessedImpl(false);
}

void ObjectStorageQueueUnorderedFileMetadata::resetProcessingImpl()
{
    setProcessedImpl(true);
}

void ObjectStorageQueueUnorderedFileMetadata::setProcessedImpl(bool remove_processing_nodes_only)
{
    /// In one zookeeper transaction do the following:
    enum RequestType
    {
        CHECK_PROCESSING_ID_PATH,
        REMOVE_PROCESSING_ID_PATH,
        REMOVE_PROCESSING_PATH,
        SET_PROCESSED_PATH,
    };

    const auto zk_client = getZooKeeper();
    Coordination::Requests requests;
    std::map<RequestType, UInt8> request_index;

    if (processing_id_version.has_value())
    {
        requests.push_back(zkutil::makeCheckRequest(processing_node_id_path, processing_id_version.value()));
        requests.push_back(zkutil::makeRemoveRequest(processing_node_id_path, processing_id_version.value()));
        requests.push_back(zkutil::makeRemoveRequest(processing_node_path, -1));

        /// The order is important:
        /// we must first check processing nodes and set processed_path the last.
        request_index[CHECK_PROCESSING_ID_PATH] = 0;
        request_index[REMOVE_PROCESSING_ID_PATH] = 1;
        request_index[REMOVE_PROCESSING_PATH] = 2;

        if (!remove_processing_nodes_only)
            request_index[SET_PROCESSED_PATH] = 3;
    }
    else if (!remove_processing_nodes_only)
    {
        request_index[SET_PROCESSED_PATH] = 0;
    }

    if (!remove_processing_nodes_only)
        requests.push_back(
            zkutil::makeCreateRequest(
                processed_node_path, node_metadata.toString(), zkutil::CreateMode::Persistent));

    Coordination::Responses responses;
    auto is_request_failed = [&](RequestType type)
    {
        if (!request_index.contains(type))
            return false;
        chassert(request_index[type] < responses.size());
        return responses[request_index[type]]->error != Coordination::Error::ZOK;
    };

    const auto code = zk_client->tryMulti(requests, responses);
    if (code == Coordination::Error::ZOK)
    {
        if (remove_processing_nodes_only)
            return;

        if (max_loading_retries
            && zk_client->tryRemove(failed_node_path + ".retriable", -1) == Coordination::Error::ZOK)
        {
            LOG_TEST(log, "Removed node {}.retriable", failed_node_path);
        }

        LOG_TRACE(log, "Moved file `{}` to processed (node path: {})", path, processed_node_path);
        return;
    }

    bool unexpected_error = false;
    std::string failure_reason;

    if (Coordination::isHardwareError(code))
    {
        failure_reason = "Lost connection to keeper";
    }
    else if (is_request_failed(CHECK_PROCESSING_ID_PATH))
    {
        /// This is normal in case of expired session with keeper.
        failure_reason = "Version of processing id node changed";
    }
    else if (is_request_failed(REMOVE_PROCESSING_ID_PATH))
    {
        /// Remove processing_id node should not actually fail
        /// because we just checked in a previous keeper request that it exists and has a certain version.
        unexpected_error = true;
        failure_reason = "Failed to remove processing id path";
    }
    else if (is_request_failed(REMOVE_PROCESSING_PATH))
    {
        /// This is normal in case of expired session with keeper as this node is ephemeral.
        failure_reason = "Failed to remove processing path";
    }
    else if (!remove_processing_nodes_only && is_request_failed(SET_PROCESSED_PATH))
    {
        unexpected_error = true;
        failure_reason = "Cannot create a persistent node in /processed since it already exists";
    }
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected state of zookeeper transaction: {}", code);

    if (unexpected_error)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{}", failure_reason);

    if (!remove_processing_nodes_only)
        LOG_WARNING(log, "Cannot set file {} as processed: {}. Reason: {}", path, code, failure_reason);
}

}
