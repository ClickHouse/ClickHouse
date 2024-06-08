#include <Storages/S3Queue/S3QueueUnorderedFileMetadata.h>
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

S3QueueUnorderedFileMetadata::S3QueueUnorderedFileMetadata(
    const std::filesystem::path & zk_path,
    const std::string & path_,
    FileStatusPtr file_status_,
    size_t max_loading_retries_,
    LoggerPtr log_)
    : S3QueueIFileMetadata(
        path_,
        /* processing_node_path */zk_path / "processing" / getNodeName(path_),
        /* processed_node_path */zk_path / "processed" / getNodeName(path_),
        /* failed_node_path */zk_path / "failed" / getNodeName(path_),
        file_status_,
        max_loading_retries_,
        log_)
{
}

std::pair<bool, S3QueueIFileMetadata::FileStatus::State> S3QueueUnorderedFileMetadata::setProcessingImpl()
{
    /// In one zookeeper transaction do the following:
    enum RequestType
    {
        /// node_name is not within processed persistent nodes
        PROCESSED_PATH_DOESNT_EXIST = 0,
        /// node_name is not within failed persistent nodes
        FAILED_PATH_DOESNT_EXIST = 2,
        /// node_name ephemeral processing node was successfully created
        CREATED_PROCESSING_PATH = 4,
        /// update processing id
        SET_PROCESSING_ID = 6,
    };

    const auto zk_client = getZooKeeper();
    processing_id = node_metadata.processing_id = getRandomASCIIString(10);
    auto processor_info = getProcessorInfo(processing_id.value());

    Coordination::Requests requests;
    requests.push_back(zkutil::makeCreateRequest(processed_node_path, "", zkutil::CreateMode::Persistent));
    requests.push_back(zkutil::makeRemoveRequest(processed_node_path, -1));
    requests.push_back(zkutil::makeCreateRequest(failed_node_path, "", zkutil::CreateMode::Persistent));
    requests.push_back(zkutil::makeRemoveRequest(failed_node_path, -1));
    requests.push_back(zkutil::makeCreateRequest(processing_node_path, node_metadata.toString(), zkutil::CreateMode::Ephemeral));

    requests.push_back(
        zkutil::makeCreateRequest(
            processing_node_id_path, processor_info, zkutil::CreateMode::Persistent, /* ignore_if_exists */true));
    requests.push_back(zkutil::makeSetRequest(processing_node_id_path, processor_info, -1));

    Coordination::Responses responses;
    const auto code = zk_client->tryMulti(requests, responses);
    auto is_request_failed = [&](RequestType type) { return responses[type]->error != Coordination::Error::ZOK; };

    if (code == Coordination::Error::ZOK)
    {
        const auto * set_response = dynamic_cast<const Coordination::SetResponse *>(responses[SET_PROCESSING_ID].get());
        processing_id_version = set_response->stat.version;
        return std::pair{true, FileStatus::State::None};
    }

    if (is_request_failed(PROCESSED_PATH_DOESNT_EXIST))
        return {false, FileStatus::State::Processed};

    if (is_request_failed(FAILED_PATH_DOESNT_EXIST))
        return {false, FileStatus::State::Failed};

    if (is_request_failed(CREATED_PROCESSING_PATH))
        return {false, FileStatus::State::Processing};

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected state of zookeeper transaction: {}", magic_enum::enum_name(code));
}

void S3QueueUnorderedFileMetadata::setProcessedAtStartRequests(
    Coordination::Requests & requests,
    const zkutil::ZooKeeperPtr &)
{
    requests.push_back(
        zkutil::makeCreateRequest(
            processed_node_path, node_metadata.toString(), zkutil::CreateMode::Persistent));
}

void S3QueueUnorderedFileMetadata::setProcessedImpl()
{
    /// In one zookeeper transaction do the following:
    enum RequestType
    {
        SET_MAX_PROCESSED_PATH = 0,
        CHECK_PROCESSING_ID_PATH = 1, /// Optional.
        REMOVE_PROCESSING_ID_PATH = 2, /// Optional.
        REMOVE_PROCESSING_PATH = 3, /// Optional.
    };

    const auto zk_client = getZooKeeper();
    std::string failure_reason;

    Coordination::Requests requests;
    requests.push_back(
        zkutil::makeCreateRequest(
            processed_node_path, node_metadata.toString(), zkutil::CreateMode::Persistent));

    if (processing_id_version.has_value())
    {
        requests.push_back(zkutil::makeCheckRequest(processing_node_id_path, processing_id_version.value()));
        requests.push_back(zkutil::makeRemoveRequest(processing_node_id_path, processing_id_version.value()));
        requests.push_back(zkutil::makeRemoveRequest(processing_node_path, -1));
    }

    Coordination::Responses responses;
    auto is_request_failed = [&](RequestType type) { return responses[type]->error != Coordination::Error::ZOK; };

    const auto code = zk_client->tryMulti(requests, responses);
    if (code == Coordination::Error::ZOK)
    {
        if (max_loading_retries)
            zk_client->tryRemove(failed_node_path + ".retriable", -1);

        LOG_TRACE(log, "Moved file `{}` to processed (node path: {})", path, processed_node_path);
        return;
    }

    if (Coordination::isHardwareError(code))
        failure_reason = "Lost connection to keeper";
    else if (is_request_failed(SET_MAX_PROCESSED_PATH))
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Cannot create a persistent node in /processed since it already exists");
    else if (is_request_failed(CHECK_PROCESSING_ID_PATH))
        failure_reason = "Version of processing id node changed";
    else if (is_request_failed(REMOVE_PROCESSING_PATH))
        failure_reason = "Failed to remove processing path";
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected state of zookeeper transaction: {}", code);

    LOG_WARNING(log, "Cannot set file {} as processed: {}. Reason: {}", path, code, failure_reason);
}

}
