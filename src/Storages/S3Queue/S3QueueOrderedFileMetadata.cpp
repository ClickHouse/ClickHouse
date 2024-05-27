#include "S3QueueOrderedFileMetadata.h"
#include <Common/SipHash.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>
#include <Common/DNSResolver.h>
#include <Interpreters/Context.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    OrderedFileMetadata::Bucket getBucketForPathImpl(const std::string & path, size_t buckets_num)
    {
        return sipHash64(path) % buckets_num;
    }

    std::string getProcessedPath(const std::filesystem::path & zk_path, const std::string & path, size_t buckets_num)
    {
        if (buckets_num > 1)
            return zk_path / "buckets" / toString(getBucketForPathImpl(path, buckets_num)) / "processed";
        else
            return zk_path / "processed";
    }

    zkutil::ZooKeeperPtr getZooKeeper()
    {
        return Context::getGlobalContextInstance()->getZooKeeper();
    }
}

struct OrderedFileMetadata::BucketHolder
{
    BucketHolder(const std::string & bucket_lock_path_, zkutil::ZooKeeperPtr zk_client_)
        : bucket_lock_path(bucket_lock_path_), zk_client(zk_client_) {}

    void release()
    {
        if (released)
            return;
        released = true;
        zk_client->remove(bucket_lock_path);
    }

    ~BucketHolder()
    {
        try
        {
            release();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

private:
    const std::string bucket_lock_path;
    const zkutil::ZooKeeperPtr zk_client;
    bool released = false;
};

OrderedFileMetadata::OrderedFileMetadata(
    const std::filesystem::path & zk_path,
    const std::string & path_,
    FileStatusPtr file_status_,
    size_t buckets_num_,
    size_t max_loading_retries_,
    LoggerPtr log_)
    : IFileMetadata(
        path_,
        /* processing_node_path */zk_path / "processing" / getNodeName(path_),
        /* processed_node_path */getProcessedPath(zk_path, path_, buckets_num_),
        /* failed_node_path */zk_path / "failed" / getNodeName(path_),
        file_status_,
        max_loading_retries_,
        log_)
    , buckets_num(buckets_num_)
{
}

OrderedFileMetadata::Bucket OrderedFileMetadata::getBucketForPath(const std::string & path_, size_t buckets_num)
{
    return getBucketForPathImpl(path_, buckets_num);
}

static std::string getProcessorInfo(const std::string & processor_id)
{
    /// Add information which will be useful for debugging just in case.
    /// TODO: add it for Unordered mode as well.
    Poco::JSON::Object json;
    json.set("hostname", DNSResolver::instance().getHostName());
    json.set("processor_id", processor_id);

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(json, oss);
    return oss.str();
}

OrderedFileMetadata::BucketHolderPtr OrderedFileMetadata::tryAcquireBucket(
    const std::filesystem::path & zk_path,
    const Bucket & bucket,
    const Processor & processor)
{
    const auto zk_client = getZooKeeper();
    const auto bucket_lock_path = zk_path / "buckets" / toString(bucket) / "lock";
    const auto processor_info = getProcessorInfo(processor);

    /// TODO: move this somewhere so that we do not do it each time.
    zk_client->createAncestors(bucket_lock_path);

    auto code = zk_client->tryCreate(bucket_lock_path, processor_info, zkutil::CreateMode::Ephemeral);
    if (code == Coordination::Error::ZOK)
        return std::make_shared<BucketHolder>(bucket_lock_path, zk_client);

    if (code == Coordination::Error::ZNODEEXISTS)
        return nullptr;

    if (Coordination::isHardwareError(code))
        return nullptr;

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected error: {}", magic_enum::enum_name(code));
}

std::pair<bool, IFileMetadata::FileStatus::State> OrderedFileMetadata::setProcessingImpl()
{
    /// In one zookeeper transaction do the following:
    enum RequestType
    {
        /// node_name is not within failed persistent nodes
        FAILED_PATH_DOESNT_EXIST = 0,
        /// node_name ephemeral processing node was successfully created
        CREATED_PROCESSING_PATH = 2,
        /// max_processed_node version did not change
        CHECKED_MAX_PROCESSED_PATH = 3,
    };

    processing_id = node_metadata.processing_id = getRandomASCIIString(10);
    const auto zk_client = getZooKeeper();
    while (true)
    {
        NodeMetadata processed_node;
        Coordination::Stat processed_node_stat;
        bool has_processed_node = getMaxProcessedFile(processed_node, &processed_node_stat, zk_client);
        if (has_processed_node)
        {
            LOG_TEST(log, "Current max processed file {} from path: {}",
                        processed_node.file_path, processed_node_path);

            if (!processed_node.file_path.empty() && path <= processed_node.file_path)
            {
                return {false, FileStatus::State::Processed};
            }
        }

        Coordination::Requests requests;
        requests.push_back(zkutil::makeCreateRequest(failed_node_path, "", zkutil::CreateMode::Persistent));
        requests.push_back(zkutil::makeRemoveRequest(failed_node_path, -1));
        requests.push_back(zkutil::makeCreateRequest(processing_node_path, node_metadata.toString(), zkutil::CreateMode::Ephemeral));
        if (has_processed_node)
        {
            requests.push_back(zkutil::makeCheckRequest(processed_node_path, processed_node_stat.version));
        }
        else
        {
            requests.push_back(zkutil::makeCreateRequest(processed_node_path, "", zkutil::CreateMode::Persistent));
            requests.push_back(zkutil::makeRemoveRequest(processed_node_path, -1));
        }

        Coordination::Responses responses;
        const auto code = zk_client->tryMulti(requests, responses);
        auto is_request_failed = [&](RequestType type) { return responses[type]->error != Coordination::Error::ZOK; };

        if (code == Coordination::Error::ZOK)
            return {true, FileStatus::State::None};

        if (is_request_failed(FAILED_PATH_DOESNT_EXIST))
            return {false, FileStatus::State::Failed};

        if (is_request_failed(CREATED_PROCESSING_PATH))
            return {false, FileStatus::State::Processing};

        if (is_request_failed(CHECKED_MAX_PROCESSED_PATH))
        {
            LOG_TEST(log, "Version of max processed file changed: {}. Will retry for file `{}`", code, path);
            continue;
        }

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected response state: {}", code);
    }
}

void OrderedFileMetadata::setProcessedImpl()
{
    LOG_TRACE(log, "Setting file `{}` as processed (at {})", path, processed_node_path);

    const auto zk_client = getZooKeeper();
    const auto node_metadata_str = node_metadata.toString();
    while (true)
    {
        NodeMetadata processed_node;
        Coordination::Stat processed_node_stat;
        Coordination::Requests requests;

        if (getMaxProcessedFile(processed_node, &processed_node_stat, zk_client))
        {
            if (!processed_node.file_path.empty() && path <= processed_node.file_path)
            {
                LOG_TRACE(log, "File {} is already processed, current max processed file: {}", path, processed_node.file_path);
                return;
            }
            requests.push_back(zkutil::makeSetRequest(processed_node_path, node_metadata_str, processed_node_stat.version));
        }
        else
            requests.push_back(zkutil::makeCreateRequest(processed_node_path, node_metadata_str, zkutil::CreateMode::Persistent));

        if (processing_id.has_value())
            requests.push_back(zkutil::makeRemoveRequest(processing_node_path, -1));

        Coordination::Responses responses;
        auto code = zk_client->tryMulti(requests, responses);
        if (code == Coordination::Error::ZOK)
        {
            if (max_loading_retries)
                zk_client->tryRemove(failed_node_path + ".retriable", -1);

            LOG_TRACE(log, "Moved file `{}` to processed", path);
            return;
        }

        /// Failed to update max processed node, retry.
        if (!responses.empty() && responses[0]->error != Coordination::Error::ZOK)
        {
            LOG_TRACE(log, "Failed to update processed node for path {} ({}). Will retry.",
                    path, magic_enum::enum_name(responses[0]->error));
            continue;
        }

        LOG_WARNING(log, "Cannot set file ({}) as processed since processing node "
                    "does not exist with expected processing id does not exist, "
                    "this could be a result of expired zookeeper session", path);
        return;
    }
}

}
