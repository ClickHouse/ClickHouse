#include "S3QueueUnorderedFileMetadata.h"
#include <Common/getRandomASCIIString.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace
{
    zkutil::ZooKeeperPtr getZooKeeper()
    {
        return Context::getGlobalContextInstance()->getZooKeeper();
    }
}

UnorderedFileMetadata::UnorderedFileMetadata(
    const std::filesystem::path & zk_path,
    const std::string & path_,
    FileStatusPtr file_status_,
    size_t max_loading_retries_,
    LoggerPtr log_)
    : IFileMetadata(
        path_,
        /* processing_node_path */zk_path / "processing" / getNodeName(path_),
        /* processed_node_path */zk_path / "processed" / getNodeName(path_),
        /* failed_node_path */zk_path / "failed" / getNodeName(path_),
        file_status_,
        max_loading_retries_,
        log_)
{
}

std::pair<bool, IFileMetadata::FileStatus::State> UnorderedFileMetadata::setProcessingImpl()
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
    };

    const auto zk_client = getZooKeeper();
    processing_id = node_metadata.processing_id = getRandomASCIIString(10);

    Coordination::Requests requests;
    requests.push_back(zkutil::makeCreateRequest(processed_node_path, "", zkutil::CreateMode::Persistent));
    requests.push_back(zkutil::makeRemoveRequest(processed_node_path, -1));
    requests.push_back(zkutil::makeCreateRequest(failed_node_path, "", zkutil::CreateMode::Persistent));
    requests.push_back(zkutil::makeRemoveRequest(failed_node_path, -1));
    requests.push_back(zkutil::makeCreateRequest(processing_node_path, node_metadata.toString(), zkutil::CreateMode::Ephemeral));

    Coordination::Responses responses;
    const auto code = zk_client->tryMulti(requests, responses);
    auto is_request_failed = [&](RequestType type) { return responses[type]->error != Coordination::Error::ZOK; };

    if (code == Coordination::Error::ZOK)
        return std::pair{true, FileStatus::State::None};

    if (is_request_failed(PROCESSED_PATH_DOESNT_EXIST))
        return {false, FileStatus::State::Processed};

    if (is_request_failed(FAILED_PATH_DOESNT_EXIST))
        return {false, FileStatus::State::Failed};

    if (is_request_failed(CREATED_PROCESSING_PATH))
        return {false, FileStatus::State::Processing};

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected state of zookeeper transaction: {}", magic_enum::enum_name(code));
}

void UnorderedFileMetadata::setProcessedImpl()
{
    const auto zk_client = getZooKeeper();
    const auto node_metadata_str = node_metadata.toString();

    Coordination::Requests requests;
    requests.push_back(zkutil::makeCreateRequest(processed_node_path, node_metadata_str, zkutil::CreateMode::Persistent));

    if (processing_id.has_value())
        requests.push_back(zkutil::makeRemoveRequest(processing_node_path, -1));

    Coordination::Responses responses;
    const auto code = zk_client->tryMulti(requests, responses);
    if (code == Coordination::Error::ZOK)
    {
        if (max_loading_retries)
            zk_client->tryRemove(failed_node_path + ".retriable", -1);

        LOG_TRACE(log, "Moved file `{}` to processed (node path: {})", path, processed_node_path);
        return;
    }

    if (!responses.empty() && responses[0]->error != Coordination::Error::ZOK)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Cannot create a persistent node in /processed since it already exists");
    }

    LOG_WARNING(log,
                "Cannot set file ({}) as processed since ephemeral node in /processing (code: {})"
                "does not exist with expected id, "
                "this could be a result of expired zookeeper session", path, responses[1]->error);
}

}
