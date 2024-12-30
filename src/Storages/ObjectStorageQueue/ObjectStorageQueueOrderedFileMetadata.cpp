#include <Storages/ObjectStorageQueue/ObjectStorageQueueOrderedFileMetadata.h>
#include <Common/SipHash.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{
    ObjectStorageQueueOrderedFileMetadata::Bucket getBucketForPathImpl(const std::string & path, size_t buckets_num)
    {
        return sipHash64(path) % buckets_num;
    }

    std::string getProcessedPathWithBucket(const std::filesystem::path & zk_path, size_t bucket)
    {
        return zk_path / "buckets" / toString(bucket) / "processed";
    }

    std::string getProcessedPathWithoutBucket(const std::filesystem::path & zk_path)
    {
        return zk_path / "processed";
    }

    std::string getProcessedPath(const std::filesystem::path & zk_path, const std::string & path, size_t buckets_num)
    {
        if (buckets_num > 1)
            return getProcessedPathWithBucket(zk_path, getBucketForPathImpl(path, buckets_num));
        return getProcessedPathWithoutBucket(zk_path);
    }

    zkutil::ZooKeeperPtr getZooKeeper()
    {
        return Context::getGlobalContextInstance()->getZooKeeper();
    }
}

ObjectStorageQueueOrderedFileMetadata::BucketHolder::BucketHolder(
    const Bucket & bucket_,
    int bucket_version_,
    const std::string & bucket_lock_path_,
    const std::string & bucket_lock_id_path_,
    zkutil::ZooKeeperPtr zk_client_,
    LoggerPtr log_)
    : bucket_info(std::make_shared<BucketInfo>(BucketInfo{
        .bucket = bucket_,
        .bucket_version = bucket_version_,
        .bucket_lock_path = bucket_lock_path_,
        .bucket_lock_id_path = bucket_lock_id_path_}))
    , zk_client(zk_client_)
    , log(log_)
{
}

void ObjectStorageQueueOrderedFileMetadata::BucketHolder::release()
{
    if (released)
        return;

    released = true;

    LOG_TEST(log, "Releasing bucket {}, version {}",
             bucket_info->bucket, bucket_info->bucket_version);

    Coordination::Requests requests;
    /// Check that bucket lock version has not changed
    /// (which could happen if session had expired as bucket_lock_path is ephemeral node).
    requests.push_back(zkutil::makeCheckRequest(bucket_info->bucket_lock_id_path, bucket_info->bucket_version));
    /// Remove bucket lock.
    requests.push_back(zkutil::makeRemoveRequest(bucket_info->bucket_lock_path, -1));

    Coordination::Responses responses;
    const auto code = zk_client->tryMulti(requests, responses);

    if (code == Coordination::Error::ZOK)
        LOG_TEST(log, "Released bucket {}, version {}",
                 bucket_info->bucket, bucket_info->bucket_version);
    else
        LOG_TRACE(log,
                  "Failed to release bucket {}, version {}: {}. "
                  "This is normal if keeper session expired.",
                  bucket_info->bucket, bucket_info->bucket_version, code);

    zkutil::KeeperMultiException::check(code, requests, responses);
}

ObjectStorageQueueOrderedFileMetadata::BucketHolder::~BucketHolder()
{
    if (!released)
        LOG_TEST(log, "Releasing bucket ({}) holder in destructor", bucket_info->bucket);

    try
    {
        release();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

ObjectStorageQueueOrderedFileMetadata::ObjectStorageQueueOrderedFileMetadata(
    const std::filesystem::path & zk_path_,
    const std::string & path_,
    FileStatusPtr file_status_,
    BucketInfoPtr bucket_info_,
    size_t buckets_num_,
    size_t max_loading_retries_,
    LoggerPtr log_)
    : ObjectStorageQueueIFileMetadata(
        path_,
        /* processing_node_path */zk_path_ / "processing" / getNodeName(path_),
        /* processed_node_path */getProcessedPath(zk_path_, path_, buckets_num_),
        /* failed_node_path */zk_path_ / "failed" / getNodeName(path_),
        file_status_,
        max_loading_retries_,
        log_)
    , buckets_num(buckets_num_)
    , zk_path(zk_path_)
    , bucket_info(bucket_info_)
{
}

std::vector<std::string> ObjectStorageQueueOrderedFileMetadata::getMetadataPaths(size_t buckets_num)
{
    if (buckets_num > 1)
    {
        std::vector<std::string> paths{"buckets", "failed", "processing"};
        for (size_t i = 0; i < buckets_num; ++i)
            paths.push_back("buckets/" + toString(i));
        return paths;
    }
    return {"failed", "processing"};
}

bool ObjectStorageQueueOrderedFileMetadata::getMaxProcessedFile(
    NodeMetadata & result,
    Coordination::Stat * stat,
    const zkutil::ZooKeeperPtr & zk_client)
{
    return getMaxProcessedFile(result, stat, processed_node_path, zk_client);
}

bool ObjectStorageQueueOrderedFileMetadata::getMaxProcessedFile(
    NodeMetadata & result,
    Coordination::Stat * stat,
    const std::string & processed_node_path_,
    const zkutil::ZooKeeperPtr & zk_client)
{
    std::string data;
    if (zk_client->tryGet(processed_node_path_, data, stat))
    {
        if (!data.empty())
            result = NodeMetadata::fromString(data);
        return true;
    }
    return false;
}

ObjectStorageQueueOrderedFileMetadata::Bucket ObjectStorageQueueOrderedFileMetadata::getBucketForPath(const std::string & path_, size_t buckets_num)
{
    return getBucketForPathImpl(path_, buckets_num);
}

ObjectStorageQueueOrderedFileMetadata::BucketHolderPtr ObjectStorageQueueOrderedFileMetadata::tryAcquireBucket(
    const std::filesystem::path & zk_path,
    const Bucket & bucket,
    const Processor & processor,
    LoggerPtr log_)
{
    const auto zk_client = getZooKeeper();
    const auto bucket_lock_path = zk_path / "buckets" / toString(bucket) / "lock";
    const auto bucket_lock_id_path = zk_path / "buckets" / toString(bucket) / "lock_id";
    const auto processor_info = getProcessorInfo(processor);

    while (true)
    {
        Coordination::Requests requests;

        /// Create bucket lock node as ephemeral node.
        requests.push_back(zkutil::makeCreateRequest(bucket_lock_path, "", zkutil::CreateMode::Ephemeral));

        /// Create bucket lock id node as persistent node if it does not exist yet.
        /// Update bucket lock id path. We use its version as a version of ephemeral bucket lock node.
        /// (See comment near ObjectStorageQueueIFileMetadata::processing_node_version).
        bool create_if_not_exists_enabled = zk_client->isFeatureEnabled(DB::KeeperFeatureFlag::CREATE_IF_NOT_EXISTS);
        if (create_if_not_exists_enabled)
        {
            requests.push_back(
                zkutil::makeCreateRequest(bucket_lock_id_path, "", zkutil::CreateMode::Persistent, /* ignore_if_exists */ true));
        }
        else if (!zk_client->exists(bucket_lock_id_path))
        {
            requests.push_back(zkutil::makeCreateRequest(bucket_lock_id_path, "", zkutil::CreateMode::Persistent));
        }

        requests.push_back(zkutil::makeSetRequest(bucket_lock_id_path, processor_info, -1));

        Coordination::Responses responses;
        const auto code = zk_client->tryMulti(requests, responses);
        if (code == Coordination::Error::ZOK)
        {
            const auto & set_response = dynamic_cast<const Coordination::SetResponse &>(*responses.back());
            const auto bucket_lock_version = set_response.stat.version;

            LOG_TEST(
                log_,
                "Processor {} acquired bucket {} for processing (bucket lock version: {})",
                processor, bucket, bucket_lock_version);

            return std::make_shared<BucketHolder>(
                bucket,
                bucket_lock_version,
                bucket_lock_path,
                bucket_lock_id_path,
                zk_client,
                log_);
        }

        if (responses[0]->error == Coordination::Error::ZNODEEXISTS)
            return nullptr;

        if (create_if_not_exists_enabled)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected error: {}", code);

        LOG_INFO(log_, "Bucket lock id path was probably created or removed while acquiring the bucket (error code: {}), will retry", code);
    }
}

std::pair<bool, ObjectStorageQueueIFileMetadata::FileStatus::State> ObjectStorageQueueOrderedFileMetadata::setProcessingImpl()
{
    const auto zk_client = getZooKeeper();
    processing_id = node_metadata.processing_id = getRandomASCIIString(10);
    auto processor_info = getProcessorInfo(processing_id.value());

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
        const auto failed_path_doesnt_exist_idx = 0;
        zkutil::addCheckNotExistsRequest(requests, *zk_client, failed_node_path);
        const auto create_processing_path_idx = requests.size();
        requests.push_back(zkutil::makeCreateRequest(processing_node_path, node_metadata.toString(), zkutil::CreateMode::Ephemeral));

        bool create_if_not_exists_enabled = zk_client->isFeatureEnabled(DB::KeeperFeatureFlag::CREATE_IF_NOT_EXISTS);
        if (create_if_not_exists_enabled)
        {
            requests.push_back(
                zkutil::makeCreateRequest(processing_node_id_path, "", zkutil::CreateMode::Persistent, /* ignore_if_exists */ true));
        }
        else if (!zk_client->exists(processing_node_id_path))
        {
            requests.push_back(zkutil::makeCreateRequest(processing_node_id_path, "", zkutil::CreateMode::Persistent));
        }

        requests.push_back(zkutil::makeSetRequest(processing_node_id_path, processor_info, -1));
        const auto set_processing_id_idx = requests.size() - 1;

        std::optional<size_t> check_bucket_version_idx;
        if (bucket_info)
        {
            check_bucket_version_idx.emplace(requests.size());
            requests.push_back(zkutil::makeCheckRequest(bucket_info->bucket_lock_id_path, bucket_info->bucket_version));
        }

        /// TODO: for ordered processing with buckets it should be enough to check only bucket lock version,
        /// so may be remove creation and check for processing_node_id if bucket_info is set?

        auto check_max_processed_path = requests.size();
        if (has_processed_node)
            requests.push_back(zkutil::makeCheckRequest(processed_node_path, processed_node_stat.version));
        else
            zkutil::addCheckNotExistsRequest(requests, *zk_client, processed_node_path);

        Coordination::Responses responses;
        const auto code = zk_client->tryMulti(requests, responses);
        auto has_request_failed = [&](size_t request_index) { return responses[request_index]->error != Coordination::Error::ZOK; };

        if (code == Coordination::Error::ZOK)
        {
            const auto * set_response = dynamic_cast<const Coordination::SetResponse *>(responses[set_processing_id_idx].get());
            processing_id_version = set_response->stat.version;
            return {true, FileStatus::State::None};
        }

        if (has_request_failed(failed_path_doesnt_exist_idx))
            return {false, FileStatus::State::Failed};

        if (has_request_failed(create_processing_path_idx))
            return {false, FileStatus::State::Processing};

        if (check_bucket_version_idx.has_value() && has_request_failed(*check_bucket_version_idx))
        {
            LOG_TEST(log, "Version of bucket lock changed: {}. Will retry for file `{}`", code, path);
            continue;
        }

        if (has_request_failed(check_max_processed_path))
        {
            LOG_TEST(log, "Version of max processed file changed: {}. Will retry for file `{}`", code, path);
            continue;
        }

        if (create_if_not_exists_enabled)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected state of zookeeper transaction: {}", code);

        /// most likely the processing node id path node was removed or created so let's try again
        LOG_TRACE(log, "Retrying setProcessing because processing node id path is unexpectedly missing or was created (error code: {})", code);
    }
}

void ObjectStorageQueueOrderedFileMetadata::setProcessedAtStartRequests(
    Coordination::Requests & requests,
    const zkutil::ZooKeeperPtr & zk_client)
{
    if (buckets_num > 1)
    {
        for (size_t i = 0; i < buckets_num; ++i)
        {
            auto path = getProcessedPathWithBucket(zk_path, i);
            setProcessedRequests(requests, zk_client, path, /* ignore_if_exists */true);
        }
    }
    else
    {
        setProcessedRequests(requests, zk_client, processed_node_path, /* ignore_if_exists */true);
    }
}

void ObjectStorageQueueOrderedFileMetadata::setProcessedRequests(
    Coordination::Requests & requests,
    const zkutil::ZooKeeperPtr & zk_client,
    const std::string & processed_node_path_,
    bool ignore_if_exists)
{
    NodeMetadata processed_node;
    Coordination::Stat processed_node_stat;
    if (getMaxProcessedFile(processed_node, &processed_node_stat, processed_node_path_, zk_client))
    {
        LOG_TEST(log, "Current max processed file: {}, condition less: {}",
                 processed_node.file_path, bool(path <= processed_node.file_path));

        if (!processed_node.file_path.empty() && path <= processed_node.file_path)
        {
            LOG_TRACE(log, "File {} is already processed, current max processed file: {}", path, processed_node.file_path);

            if (ignore_if_exists)
                return;

            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "File ({}) is already processed, while expected it not to be (path: {})",
                path, processed_node_path_);
        }
        requests.push_back(zkutil::makeSetRequest(processed_node_path_, node_metadata.toString(), processed_node_stat.version));
    }
    else
    {
        LOG_TEST(log, "Max processed file does not exist, creating at: {}", processed_node_path_);
        requests.push_back(zkutil::makeCreateRequest(processed_node_path_, node_metadata.toString(), zkutil::CreateMode::Persistent));
    }

    if (processing_id_version.has_value())
    {
        requests.push_back(zkutil::makeCheckRequest(processing_node_id_path, processing_id_version.value()));
        requests.push_back(zkutil::makeRemoveRequest(processing_node_id_path, processing_id_version.value()));
        requests.push_back(zkutil::makeRemoveRequest(processing_node_path, -1));
    }
}

void ObjectStorageQueueOrderedFileMetadata::setProcessedImpl()
{
    /// In one zookeeper transaction do the following:
    enum RequestType
    {
        CHECK_PROCESSING_ID_PATH = 0,
        REMOVE_PROCESSING_ID_PATH = 1,
        REMOVE_PROCESSING_PATH = 2,
        SET_MAX_PROCESSED_PATH = 3,
    };

    const auto zk_client = getZooKeeper();
    std::string failure_reason;

    while (true)
    {
        Coordination::Requests requests;
        setProcessedRequests(requests, zk_client, processed_node_path, /* ignore_if_exists */false);

        Coordination::Responses responses;
        auto is_request_failed = [&](RequestType type) { return responses[type]->error != Coordination::Error::ZOK; };

        auto code = zk_client->tryMulti(requests, responses);
        if (code == Coordination::Error::ZOK)
        {
            if (max_loading_retries
                && zk_client->tryRemove(failed_node_path + ".retriable", -1) == Coordination::Error::ZOK)
            {
                LOG_TEST(log, "Removed node {}.retriable", failed_node_path);
            }
            return;
        }

        bool unexpected_error = false;
        if (Coordination::isHardwareError(code))
            failure_reason = "Lost connection to keeper";
        else if (is_request_failed(CHECK_PROCESSING_ID_PATH))
            failure_reason = "Version of processing id node changed";
        else if (is_request_failed(REMOVE_PROCESSING_PATH))
        {
            /// Remove processing_id node should not actually fail
            /// because we just checked in a previous keeper request that it exists and has a certain version.
            unexpected_error = true;
            failure_reason = "Failed to remove processing id path";
        }
        else if (is_request_failed(SET_MAX_PROCESSED_PATH))
        {
            LOG_TRACE(log, "Cannot set file {} as processed. "
                      "Failed to update processed node: {}. "
                      "Will retry.", path, code);
            continue;
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected state of zookeeper transaction: {}", code);

        if (unexpected_error)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "{}", failure_reason);

        LOG_WARNING(log, "Cannot set file {} as processed: {}. Reason: {}", path, code, failure_reason);
        return;
    }
}

void ObjectStorageQueueOrderedFileMetadata::migrateToBuckets(const std::string & zk_path, size_t value)
{
    auto zk_client = getZooKeeper();
    const auto log = getLogger("ObjectStorageQueueOrderedFileMetadata");
    const size_t retries = 1000;
    Coordination::Error code = Coordination::Error::ZOK;

    for (size_t try_num = 0; try_num <= retries; ++try_num)
    {
        const auto old_processed_path = getProcessedPathWithoutBucket(zk_path);
        NodeMetadata processed_node;
        Coordination::Stat processed_node_stat;
        bool has_processed_node = ObjectStorageQueueOrderedFileMetadata::getMaxProcessedFile(
            processed_node,
            &processed_node_stat,
            old_processed_path,
            zk_client);

        if (!has_processed_node)
        {
            LOG_TRACE(log, "Processed node does not exist at path {}", old_processed_path);
            return;
        }

        Coordination::Requests requests;
        requests.push_back(zkutil::makeRemoveRequest(old_processed_path, processed_node_stat.version));

        for (size_t i = 0; i < value; ++i)
        {
            const auto new_processed_path = getProcessedPathWithBucket(zk_path, /* bucket */i);
            zk_client->createAncestors(new_processed_path);

            requests.push_back(zkutil::makeCreateRequest(
                                new_processed_path,
                                processed_node.toString(),
                                zkutil::CreateMode::Persistent));
        }

        Coordination::Responses responses;
        code = zk_client->tryMulti(requests, responses);
        if (code == Coordination::Error::ZOK)
        {
            LOG_TRACE(log,
                      "Successfully migrated metadata from {} to {} buckets",
                      old_processed_path, value);
            return;
        }

        if (Coordination::isHardwareError(code))
        {
            if (try_num < retries)
            {
                LOG_TRACE(log, "Keeper session expired while updating buckets in keeper, will retry");
                zk_client = getZooKeeper();
                continue;
            }
            else
                throw zkutil::KeeperException(code);
        }

        if (responses[0]->error != Coordination::Error::ZOK)
        {
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                            "Cannot change old metadata structure, "
                            "because someone is modifying it concurrently");
        }

        if (responses[0]->error == Coordination::Error::ZNONODE)
        {
            LOG_TRACE(log,
                      "Old processed node no longer exists, "
                      "metadata must have been migrated concurrently, will recheck");
            continue;
        }

        auto it = std::find_if(
            responses.begin(), responses.end(),
            [](const auto & response) { return response->error != Coordination::Error::ZOK; });

        chassert(it != responses.end());

        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Unexpected error: {} (failed request id: {})",
                        code, std::distance(responses.begin(), responses.end()));
    }

    throw zkutil::KeeperException(code);
}

}
