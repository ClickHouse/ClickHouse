#include <Storages/ObjectStorageQueue/ObjectStorageQueueUnorderedFileMetadata.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueMetadata.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ObjectStorageQueueUnorderedFileMetadata::ObjectStorageQueueUnorderedFileMetadata(
    const std::filesystem::path & zk_path,
    const std::string & path_,
    FileStatusPtr file_status_,
    size_t max_loading_retries_,
    std::atomic<size_t> & metadata_ref_count_,
    bool use_persistent_processing_nodes_,
    LoggerPtr log_)
    : ObjectStorageQueueIFileMetadata(
        path_,
        /* processing_node_path */zk_path / "processing" / getNodeName(path_),
        /* processed_node_path */zk_path / "processed" / getNodeName(path_),
        /* failed_node_path */zk_path / "failed" / getNodeName(path_),
        file_status_,
        max_loading_retries_,
        metadata_ref_count_,
        use_persistent_processing_nodes_,
        log_)
{
}

ObjectStorageQueueUnorderedFileMetadata::SetProcessingResponseIndexes
ObjectStorageQueueUnorderedFileMetadata::prepareProcessingRequestsImpl(
    Coordination::Requests & requests,
    const std::string & processing_id)
{
    auto zk_client = ObjectStorageQueueMetadata::getZooKeeper(log);
    processor_info = getProcessorInfo(processing_id);

    SetProcessingResponseIndexes result;

    /// 1. Check processed node does not exist
    result.processed_path_doesnt_exist_idx = requests.size();
    zkutil::addCheckNotExistsRequest(requests, *zk_client, processed_node_path);

    /// 2. Check failed node does not exist
    result.failed_path_doesnt_exist_idx = requests.size();
    zkutil::addCheckNotExistsRequest(requests, *zk_client, failed_node_path);

    /// 3. Create processing node
    result.create_processing_node_idx = requests.size();
    requests.push_back(
        zkutil::makeCreateRequest(
            processing_node_path,
            processor_info,
            zkutil::CreateMode::Persistent));

    return result;
}

std::pair<bool, ObjectStorageQueueIFileMetadata::FileStatus::State> ObjectStorageQueueUnorderedFileMetadata::setProcessingImpl()
{
    Coordination::Requests requests;
    auto result = prepareProcessingRequestsImpl(requests, generateProcessingID());

    Coordination::Responses responses;
    Coordination::Error code;

    auto zk_retry = ObjectStorageQueueMetadata::getKeeperRetriesControl(log);
    zk_retry.retryLoop([&]
    {
        auto zk_client = ObjectStorageQueueMetadata::getZooKeeper(log);
        std::string data;
        /// If it is a retry, we could have failed after actually successfully executing the requests.
        /// So here we check if we succeeded by checking `processor_info` of the processing node.
        if (zk_retry.isRetry() && zk_client->tryGet(processing_node_path, data))
        {
            chassert(!data.empty());
            if (data == processor_info)
            {
                LOG_TRACE(log, "Considering operation as succeeded");
                code = Coordination::Error::ZOK;
                return;
            }
        }
        code = zk_client->tryMulti(requests, responses);
    });

    if (code == Coordination::Error::ZOK)
    {
        return std::pair{true, FileStatus::State::None};
    }

    auto has_request_failed = [&](size_t request_index)
    {
        return responses[request_index]->error != Coordination::Error::ZOK;
    };

    if (has_request_failed(result.processed_path_doesnt_exist_idx))
        return {false, FileStatus::State::Processed};

    if (has_request_failed(result.failed_path_doesnt_exist_idx))
        return {false, FileStatus::State::Failed};

    if (has_request_failed(result.create_processing_node_idx))
        return {false, FileStatus::State::Processing};

    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "Failed to set file processing, last error: {}", code);
}

void ObjectStorageQueueUnorderedFileMetadata::prepareProcessedAtStartRequests(Coordination::Requests & requests)
{
    requests.push_back(
        zkutil::makeCreateRequest(
            processed_node_path, node_metadata.toString(), zkutil::CreateMode::Persistent));
}

void ObjectStorageQueueUnorderedFileMetadata::prepareProcessedRequestsImpl(Coordination::Requests & requests)
{
    requests.push_back(zkutil::makeRemoveRequest(processing_node_path, -1));
    requests.push_back(
        zkutil::makeCreateRequest(
            processed_node_path, node_metadata.toString(), zkutil::CreateMode::Persistent));
}

void ObjectStorageQueueUnorderedFileMetadata::filterOutProcessedAndFailed(
    std::vector<std::string> & paths, const std::filesystem::path & zk_path_, LoggerPtr log_)
{
    std::vector<std::string> check_paths;
    for (const auto & path : paths)
    {
        const auto node_name = getNodeName(path);
        check_paths.push_back(zk_path_ / "processed" / node_name);
        check_paths.push_back(zk_path_ / "failed" / node_name);
    }

    zkutil::ZooKeeper::MultiTryGetResponse responses;
    ObjectStorageQueueMetadata::getKeeperRetriesControl(log_).retryLoop([&]
    {
        responses = ObjectStorageQueueMetadata::getZooKeeper(log_)->tryGet(check_paths);
    });

    auto check_code = [&](auto code, const std::string & path)
    {
        if (!(code == Coordination::Error::ZOK || code == Coordination::Error::ZNONODE))
            throw zkutil::KeeperException::fromPath(code, path);
    };

    std::vector<std::string> result;
    for (size_t i = 0; i < responses.size();)
    {
        check_code(responses[i].error, check_paths[i]);
        check_code(responses[i + 1].error, check_paths[i]);

        if (responses[i].error == Coordination::Error::ZNONODE
            && responses[i + 1].error == Coordination::Error::ZNONODE)
        {
            result.push_back(std::move(paths[i / 2]));
        }
        else
        {
            LOG_TEST(log_, "Skipping file {}: {}",
                     paths[i / 2],
                     responses[i].error == Coordination::Error::ZOK ? "Processed" : "Failed");
        }
        i += 2;
    }
    paths = std::move(result);
}

}
