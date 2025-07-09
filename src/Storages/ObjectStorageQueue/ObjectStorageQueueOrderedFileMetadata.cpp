#include <Storages/ObjectStorageQueue/ObjectStorageQueueOrderedFileMetadata.h>
#include <Common/SipHash.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>
#include <Core/Field.h>
#include <Interpreters/Context.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <numeric>

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

    bool useBucketsForProcessing(size_t buckets_num)
    {
        return buckets_num > 1;
    }

    std::string getProcessedPath(const std::filesystem::path & zk_path, const std::string & path, size_t buckets_num)
    {
        if (useBucketsForProcessing(buckets_num))
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
    std::atomic<size_t> & metadata_ref_count_,
    LoggerPtr log_)
    : ObjectStorageQueueIFileMetadata(
        path_,
        /* processing_node_path */zk_path_ / "processing" / getNodeName(path_),
        /* processed_node_path */getProcessedPath(zk_path_, path_, buckets_num_),
        /* failed_node_path */zk_path_ / "failed" / getNodeName(path_),
        file_status_,
        max_loading_retries_,
        metadata_ref_count_,
        log_)
    , buckets_num(buckets_num_)
    , zk_path(zk_path_)
    , bucket_info(bucket_info_)
{
}

bool ObjectStorageQueueOrderedFileMetadata::useBucketsForProcessing() const
{
    return DB::useBucketsForProcessing(buckets_num);
}

std::vector<std::string> ObjectStorageQueueOrderedFileMetadata::getMetadataPaths(size_t buckets_num)
{
    if (DB::useBucketsForProcessing(buckets_num))
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
    const auto create_if_not_exists_enabled = zk_client->isFeatureEnabled(DB::KeeperFeatureFlag::CREATE_IF_NOT_EXISTS);

    const auto bucket_path = zk_path / "buckets" / toString(bucket);
    chassert(zk_client->exists(bucket_path), fmt::format("Bucket path {} does not exist", bucket_path.string()));
    const auto bucket_lock_path = bucket_path / "lock";
    const auto bucket_lock_id_path = bucket_path / "lock_id";

    const auto processor_info = getProcessorInfo(processor);

    while (true)
    {
        Coordination::Requests requests;

        /// Create bucket lock node as ephemeral node.
        requests.push_back(zkutil::makeCreateRequest(bucket_lock_path, "", zkutil::CreateMode::Ephemeral));

        /// Create bucket lock id node as persistent node if it does not exist yet.
        /// Update bucket lock id path. We use its version as a version of ephemeral bucket lock node.
        /// (See comment near ObjectStorageQueueIFileMetadata::processing_node_version).
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
        {
            auto failed_idx = zkutil::getFailedOpIndex(code, responses);
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Unexpected error: {}, path: {} (failed idx: {})",
                code, requests[failed_idx]->getPath(), failed_idx);
        }

        LOG_INFO(log_, "Bucket lock id path was probably created or removed "
                 "while acquiring the bucket (error code: {}), will retry", code);
    }
}

std::pair<bool, ObjectStorageQueueIFileMetadata::FileStatus::State> ObjectStorageQueueOrderedFileMetadata::setProcessingImpl()
{
    const auto zk_client = getZooKeeper();
    processing_id = node_metadata.processing_id = getRandomASCIIString(10);
    auto processor_info = getProcessorInfo(processing_id.value());

    while (true)
    {
        std::optional<NodeMetadata> processed_node;
        Coordination::Stat processed_node_stat;
        if (zk_client->isFeatureEnabled(DB::KeeperFeatureFlag::MULTI_READ))
        {
            Coordination::Requests requests;
            std::vector<std::string> paths{processed_node_path, failed_node_path};
            auto responses = zk_client->tryGet(paths);

            auto check_code = [this](auto code)
            {
                if (!(code == Coordination::Error::ZOK || code == Coordination::Error::ZNONODE))
                    throw zkutil::KeeperException::fromPath(code, path);
            };
            check_code(responses[0].error);
            check_code(responses[1].error);

            if (responses[1].error == Coordination::Error::ZOK)
            {
                LOG_TEST(log, "File {} is Failed", path);
                return {false, FileStatus::State::Failed};
            }

            if (responses[0].error == Coordination::Error::ZOK)
            {
                if (!responses[0].data.empty())
                {
                    processed_node.emplace(NodeMetadata::fromString(responses[0].data));
                    processed_node_stat = responses[0].stat;

                    LOG_TEST(log, "Current max processed file {} from path: {}",
                             processed_node->file_path, processed_node_path);

                    if (!processed_node->file_path.empty() && path <= processed_node->file_path)
                    {
                        return {false, FileStatus::State::Processed};
                    }
                }
            }
        }
        else
        {
            NodeMetadata node_metadata;
            if (getMaxProcessedFile(node_metadata, &processed_node_stat, zk_client))
            {
                if (zk_client->exists(failed_node_path))
                {
                    LOG_TEST(log, "File {} is Failed", path);
                    return {false, FileStatus::State::Failed};
                }

                processed_node.emplace(node_metadata);
                LOG_TEST(log, "Current max processed file {} from path: {}",
                         processed_node->file_path, processed_node_path);

                if (!processed_node->file_path.empty() && path <= processed_node->file_path)
                {
                    return {false, FileStatus::State::Processed};
                }
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
        if (processed_node.has_value())
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

void ObjectStorageQueueOrderedFileMetadata::prepareProcessedAtStartRequests(
    Coordination::Requests & requests,
    const zkutil::ZooKeeperPtr & zk_client)
{
    if (useBucketsForProcessing())
    {
        for (size_t i = 0; i < buckets_num; ++i)
        {
            auto path = getProcessedPathWithBucket(zk_path, i);
            prepareProcessedRequests(requests, zk_client, path, /* ignore_if_exists */true);
        }
    }
    else
    {
        prepareProcessedRequests(requests, zk_client, processed_node_path, /* ignore_if_exists */true);
    }
}

void ObjectStorageQueueOrderedFileMetadata::prepareProcessedRequests(
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
        requests.push_back(zkutil::makeRemoveRequest(processing_node_id_path, processing_id_version.value()));
        requests.push_back(zkutil::makeRemoveRequest(processing_node_path, -1));
    }
}

void ObjectStorageQueueOrderedFileMetadata::prepareProcessedRequestsImpl(Coordination::Requests & requests)
{
    const auto zk_client = getZooKeeper();
    prepareProcessedRequests(requests, zk_client, processed_node_path, /* ignore_if_exists */false);
}

void ObjectStorageQueueOrderedFileMetadata::migrateToBuckets(const std::string & zk_path, size_t value, size_t prev_value)
{
    auto zk_client = getZooKeeper();
    const auto log = getLogger("ObjectStorageQueueOrderedFileMetadata");
    const size_t retries = 1000;
    Coordination::Error code = Coordination::Error::ZOK;

    Coordination::Requests requests;
    Coordination::Responses responses;

    chassert(prev_value == 0 || prev_value == 1);

    for (size_t try_num = 0; try_num <= retries; ++try_num)
    {
        requests.clear();

        const auto old_processed_path = prev_value == 0
            ? getProcessedPathWithoutBucket(zk_path)
            : getProcessedPathWithBucket(zk_path, 0);

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

        responses.clear();
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
                throw zkutil::KeeperMultiException(code, requests, responses);
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
                      "metadata must have been migrated concurrently by another replica, will recheck");
            continue;
        }

        const auto failed_idx = zkutil::getFailedOpIndex(code, responses);
        if (prev_value != 0
            /// idx=0 - for delete node request
            /// idx=1 - for create bucket 0 request
            /// idx=2 - for create bucket 1 request
            /// Failed idx=2 means that metadata was updated concurrently by another replica,
            /// in this case we just quit.
            && failed_idx == 2
            && code == Coordination::Error::ZNODEEXISTS)
        {
            LOG_TRACE(log,
                      "Bucket node already exists, "
                      "metadata must have been migrated concurrently by another replica");
            return;
        }

        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Unexpected error: {} (failed request id: {}, path: {})",
                        code, failed_idx, requests[failed_idx]->getPath());
    }

    throw zkutil::KeeperMultiException(code, requests, responses);
}

void ObjectStorageQueueOrderedFileMetadata::filterOutProcessedAndFailed(
    std::vector<std::string> & paths,
    const std::filesystem::path & zk_path_,
    size_t buckets_num,
    LoggerPtr log_)
{
    const auto zk_client = getZooKeeper();
    const bool use_buckets_for_processing = buckets_num > 1;

    buckets_num = std::max<size_t>(buckets_num, 1);
    std::map<size_t, std::string> max_processed_file_per_bucket;

    for (size_t i = 0; i < buckets_num; ++i)
    {
        auto processed_node_path = use_buckets_for_processing
            ? getProcessedPathWithBucket(zk_path_, i)
            : getProcessedPathWithoutBucket(zk_path_);

        NodeMetadata max_processed_file;
        if (getMaxProcessedFile(max_processed_file, {}, processed_node_path, zk_client))
            max_processed_file_per_bucket[i] = std::move(max_processed_file.file_path);
    }

    std::vector<std::string> failed_paths;
    std::vector<size_t> check_paths_indexes;
    for (size_t i = 0; i < paths.size(); ++i)
    {
        const auto & path = paths[i];
        const auto bucket = use_buckets_for_processing ? getBucketForPathImpl(path, buckets_num) : 0;
        if (!max_processed_file_per_bucket.empty()
            && path <= max_processed_file_per_bucket[bucket])
        {
            LOG_TEST(log_, "Skipping file {}: Processed", path);
            continue;
        }
        failed_paths.push_back(zk_path_ / "failed" / getNodeName(path));
        check_paths_indexes.push_back(i);
    }

    std::vector<std::string> result;
    if (failed_paths.empty())
        return; /// All files are already processed.

    auto check_code = [&](auto code, const std::string & path)
    {
        if (!(code == Coordination::Error::ZOK || code == Coordination::Error::ZNONODE))
            throw zkutil::KeeperException::fromPath(code, path);
    };

    auto responses = zk_client->tryGet(failed_paths);
    for (size_t i = 0; i < responses.size(); ++i)
    {
        const auto filename = std::move(paths[check_paths_indexes[i]]);
        check_code(responses[i].error, filename);
        if (responses[i].error == Coordination::Error::ZNONODE)
        {
            result.push_back(filename);
        }
        else
        {
            LOG_TEST(log_, "Skipping file {}: Failed", filename);
        }

    }
    paths = std::move(result);
}

}
