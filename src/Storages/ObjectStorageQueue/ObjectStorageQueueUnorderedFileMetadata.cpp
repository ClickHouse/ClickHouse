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

void ObjectStorageQueueUnorderedFileMetadata::prepareProcessedAtStartRequests(
    Coordination::Requests & requests,
    const zkutil::ZooKeeperPtr &)
{
    requests.push_back(
        zkutil::makeCreateRequest(
            processed_node_path, node_metadata.toString(), zkutil::CreateMode::Persistent));
}

void ObjectStorageQueueUnorderedFileMetadata::prepareProcessedRequestsImpl(Coordination::Requests & requests)
{
    if (processing_id_version.has_value())
    {
        requests.push_back(zkutil::makeRemoveRequest(processing_node_id_path, processing_id_version.value()));
        requests.push_back(zkutil::makeRemoveRequest(processing_node_path, -1));
    }
    requests.push_back(
        zkutil::makeCreateRequest(
            processed_node_path, node_metadata.toString(), zkutil::CreateMode::Persistent));
}

std::vector<size_t> ObjectStorageQueueUnorderedFileMetadata::filterOutProcessedAndFailed(
    const std::vector<std::string> & paths, const std::filesystem::path & zk_path_, LoggerPtr log_)
{
    std::vector<std::string> check_paths;
    for (const auto & path : paths)
    {
        const auto node_name = getNodeName(path);
        check_paths.push_back(zk_path_ / "processed" / node_name);
        check_paths.push_back(zk_path_ / "failed" / node_name);
    }

    auto zk_client = getZooKeeper();
    auto responses = zk_client->tryGet(check_paths);

    auto check_code = [&](auto code, const std::string & path)
    {
        if (!(code == Coordination::Error::ZOK || code == Coordination::Error::ZNONODE))
            throw zkutil::KeeperException::fromPath(code, path);
    };

    std::vector<size_t> result;
    for (size_t i = 0; i < responses.size();)
    {
        check_code(responses[i].error, check_paths[i]);
        check_code(responses[i + 1].error, check_paths[i]);

        if (responses[i].error == Coordination::Error::ZNONODE
            && responses[i + 1].error == Coordination::Error::ZNONODE)
        {
            result.push_back(i / 2);
        }
        else
        {
            LOG_TEST(log_, "Skipping file {}: {}",
                     paths[i / 2],
                     responses[i].error == Coordination::Error::ZOK ? "Processed" : "Failed");
        }
        i += 2;
    }
    return result;
}

}
