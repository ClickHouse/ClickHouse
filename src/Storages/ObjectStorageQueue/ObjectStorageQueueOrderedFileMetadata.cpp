#include <Storages/ObjectStorageQueue/ObjectStorageQueueOrderedFileMetadata.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueMetadata.h>
#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>
#include <Common/SipHash.h>
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
}

ObjectStorageQueueOrderedFileMetadata::BucketHolder::BucketHolder(
    const Bucket & bucket_,
    const std::string & bucket_lock_path_,
    const std::string & processor_info_,
    LoggerPtr log_)
    : bucket_info(std::make_shared<BucketInfo>(BucketInfo{
        .bucket = bucket_,
        .bucket_lock_path = bucket_lock_path_,
        .processor_info = processor_info_ }))
    , log(log_)
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    ObjectStorageQueueMetadata::getKeeperRetriesControl(log).retryLoop([&]
    {
        chassert(checkBucketOwnership(ObjectStorageQueueMetadata::getZooKeeper(log)));
    });
#endif
}

bool ObjectStorageQueueOrderedFileMetadata::BucketHolder::checkBucketOwnership(std::shared_ptr<ZooKeeperWithFaultInjection> zk_client)
{
    std::string data;
    /// No retries, because they must be done on a higher level.
    if (!zk_client->tryGet(bucket_info->bucket_lock_path, data))
        return false;

    LOG_TEST(
        log, "Bucket lock node {} has owner: {}, current owner: {}",
        bucket_info->bucket_lock_path, data, bucket_info->processor_info);

    return data == bucket_info->processor_info;
}

void ObjectStorageQueueOrderedFileMetadata::BucketHolder::release()
{
    if (released)
        return;

    released = true;

    LOG_TEST(log, "Releasing bucket {}", bucket_info->bucket);

    Coordination::Error code;
    auto zk_retry = ObjectStorageQueueMetadata::getKeeperRetriesControl(log);
    zk_retry.retryLoop([&]
    {
        auto zk_client = ObjectStorageQueueMetadata::getZooKeeper(log);
        if (zk_retry.isRetry())
        {
            /// It is possible that we fail "after operation",
            /// e.g. we successfully removed the node, but did not get confirmation,
            /// but then if we retry - we can remove a newly recreated node,
            /// therefore avoid this with this check.
            if (!checkBucketOwnership(zk_client))
            {
                LOG_TEST(log, "Will not remove bucket lock node, ownership changed");
                code = Coordination::Error::ZOK;
                return;
            }
        }
        else
        {
            chassert(checkBucketOwnership(zk_client));
        }
        code = zk_client->tryRemove(bucket_info->bucket_lock_path);
    });

    if (code == Coordination::Error::ZOK)
    {
        LOG_TEST(log, "Released bucket {}", bucket_info->bucket);
        return;
    }
    else if (zk_retry.isRetry() && code == Coordination::Error::ZNONODE)
    {
        LOG_TEST(log, "Released bucket {} (has zk session loss)", bucket_info->bucket);
        return;
    }

    throw zkutil::KeeperException::fromPath(code, bucket_info->bucket_lock_path);
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
    bool use_persistent_processing_nodes_,
    LoggerPtr log_)
    : ObjectStorageQueueIFileMetadata(
        path_,
        /* processing_node_path */zk_path_ / "processing" / getNodeName(path_),
        /* processed_node_path */getProcessedPath(zk_path_, path_, buckets_num_),
        /* failed_node_path */zk_path_ / "failed" / getNodeName(path_),
        file_status_,
        max_loading_retries_,
        metadata_ref_count_,
        use_persistent_processing_nodes_,
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
        std::vector<std::string> paths{"buckets", "failed", "processing", "persistent_processing"};
        for (size_t i = 0; i < buckets_num; ++i)
            paths.push_back("buckets/" + toString(i));
        return paths;
    }
    /// We do not return "processed" node here,
    /// because we do not want it to be created in advance.
    return {"failed", "processing", "persistent_processing"};
}

bool ObjectStorageQueueOrderedFileMetadata::getMaxProcessedFile(
    NodeMetadata & result,
    Coordination::Stat * stat,
    LoggerPtr log_)
{
    return getMaxProcessedFile(result, stat, processed_node_path, log_);
}

bool ObjectStorageQueueOrderedFileMetadata::getMaxProcessedFile(
    NodeMetadata & result,
    Coordination::Stat * stat,
    const std::string & processed_node_path_,
    LoggerPtr log_)
{
    std::string data;
    bool processed_node_exists = false;
    ObjectStorageQueueMetadata::getKeeperRetriesControl(log_).retryLoop([&]
    {
        processed_node_exists = ObjectStorageQueueMetadata::getZooKeeper(log_)->tryGet(processed_node_path_, data, stat);
    });
    if (processed_node_exists)
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
    bool /*use_persistent_processing_nodes_*/,
    LoggerPtr log_)
{
    auto zk_retry = ObjectStorageQueueMetadata::getKeeperRetriesControl(log_);
    const auto bucket_path = zk_path / "buckets" / toString(bucket);

#ifdef DEBUG_OR_SANITIZER_BUILD
    bool bucket_exists = false;
    zk_retry.retryLoop([&]
    {
        bucket_exists = ObjectStorageQueueMetadata::getZooKeeper(log_)->exists(bucket_path);
    });
    chassert(bucket_exists);
#endif

    const auto bucket_lock_path = bucket_path / "lock";
    const auto processor_info = getProcessorInfo(generateProcessingID());

    Coordination::Error code;
    zk_retry.retryLoop([&]
    {
        auto zk_client = ObjectStorageQueueMetadata::getZooKeeper(log_);
        std::string data;
        /// If it is a retry, we could have failed after actually successfully executing the request.
        /// So here we check if we succeeded by checking `processor_info` of the processing node.
        if (zk_retry.isRetry() && zk_client->tryGet(bucket_lock_path, data))
        {
            chassert(!data.empty());
            if (data == processor_info)
            {
                LOG_TRACE(log_, "Considering operation as succeeded");
                code = Coordination::Error::ZOK;
                return;
            }
        }
        code = zk_client->tryCreate(bucket_lock_path, processor_info, zkutil::CreateMode::Persistent);
    });

    if (code == Coordination::Error::ZOK)
    {
        LOG_TEST(log_, "Processor {} acquired bucket {} for processing", processor_info, bucket);

        return std::make_shared<BucketHolder>(
            bucket,
            bucket_lock_path,
            processor_info,
            log_);
    }

    if (code == Coordination::Error::ZNODEEXISTS)
        return nullptr;

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to set file processing, error: {}", code);
}

std::pair<bool, ObjectStorageQueueIFileMetadata::FileStatus::State> ObjectStorageQueueOrderedFileMetadata::setProcessingImpl()
{
    auto zk_client = ObjectStorageQueueMetadata::getZooKeeper(log);
    auto zk_retry = ObjectStorageQueueMetadata::getKeeperRetriesControl(log);

    processor_info = getProcessorInfo(generateProcessingID());

    const size_t max_num_tries = 100;
    Coordination::Error code;
    for (size_t i = 0; i < max_num_tries; ++i)
    {
        std::optional<NodeMetadata> processed_node;
        Coordination::Stat processed_node_stat;
        std::optional<std::pair<bool, ObjectStorageQueueIFileMetadata::FileStatus::State>> result;
        zk_retry.retryLoop([&]
        {
            bool is_multi_read_enabled = zk_client->isFeatureEnabled(DB::KeeperFeatureFlag::MULTI_READ);
            if (is_multi_read_enabled)
            {
                Coordination::Requests requests;
                std::vector<std::string> paths{processed_node_path, failed_node_path};
                zkutil::ZooKeeper::MultiTryGetResponse responses = ObjectStorageQueueMetadata::getZooKeeper(log)->tryGet(paths);

                auto check_code = [this](auto code_)
                {
                    if (!(code_ == Coordination::Error::ZOK || code_ == Coordination::Error::ZNONODE))
                        throw zkutil::KeeperException::fromPath(code_, path);
                };
                check_code(responses[0].error);
                check_code(responses[1].error);

                if (responses[1].error == Coordination::Error::ZOK)
                {
                    LOG_TEST(log, "File {} is Failed", path);
                    result = {false, FileStatus::State::Failed};
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
                            result = {false, FileStatus::State::Processed};
                        }
                    }
                }
            }
            else
            {
                NodeMetadata node_metadata;
                if (getMaxProcessedFile(node_metadata, &processed_node_stat, log))
                {
                    bool failed_node_exists = ObjectStorageQueueMetadata::getZooKeeper(log)->exists(failed_node_path);
                    if (failed_node_exists)
                    {
                        LOG_TEST(log, "File {} is Failed", path);
                        result = {false, FileStatus::State::Failed};
                    }

                    processed_node.emplace(node_metadata);
                    LOG_TEST(log, "Current max processed file {} from path: {}",
                            processed_node->file_path, processed_node_path);

                    if (!processed_node->file_path.empty() && path <= processed_node->file_path)
                    {
                        result = {false, FileStatus::State::Processed};
                    }
                }
            }
        });

        if (result.has_value())
            return result.value();

        Coordination::Requests requests;

        /// 1. check failed node does not exist
        /// 2. create processing node
        /// 3. check max processed path is still the same

        const auto failed_path_doesnt_exist_idx = 0;
        zkutil::addCheckNotExistsRequest(requests, *zk_client, failed_node_path);

        const auto create_processing_path_idx = requests.size();
        requests.push_back(
            zkutil::makeCreateRequest(
                processing_node_path,
                processor_info,
                use_persistent_processing_nodes ? zkutil::CreateMode::Persistent : zkutil::CreateMode::Ephemeral));

        auto check_max_processed_path = requests.size();
        if (processed_node.has_value())
            requests.push_back(zkutil::makeCheckRequest(processed_node_path, processed_node_stat.version));
        else
            zkutil::addCheckNotExistsRequest(requests, *zk_client, processed_node_path);

        Coordination::Responses responses;
        zk_retry.retryLoop([&]
        {
            auto zk = ObjectStorageQueueMetadata::getZooKeeper(log);
            /// If it is a retry, we could have failed after actually successfully executing the request.
            /// So here we check if we succeeded by checking `processor_info` of the processing node.
            if (zk_retry.isRetry())
            {
                std::string data;
                if (zk->tryGet(processing_node_path, data))
                {
                    chassert(!data.empty());
                    if (data == processor_info)
                    {
                        LOG_TRACE(log, "Considering operation as succeeded");
                        code = Coordination::Error::ZOK;
                        chassert(!zk->tryGet(failed_node_path, data));
                        return;
                    }
                }
            }
            code = zk->tryMulti(requests, responses);
        });

        if (code == Coordination::Error::ZOK)
        {
            return {true, FileStatus::State::None};
        }

        auto has_request_failed = [&](size_t request_index) { return responses[request_index]->error != Coordination::Error::ZOK; };

        auto failed_idx = zkutil::getFailedOpIndex(code, responses);
        LOG_DEBUG(log, "Code: {}, failed idx: {}, failed path: {}", code, failed_idx, requests[failed_idx]->getPath());

        if (has_request_failed(failed_path_doesnt_exist_idx))
            return {false, FileStatus::State::Failed};

        if (has_request_failed(create_processing_path_idx))
            return {false, FileStatus::State::Processing};

        if (has_request_failed(check_max_processed_path))
        {
            LOG_TEST(log, "Version of max processed file changed: {}. Will retry for file `{}`", code, path);
            continue;
        }

        /// most likely the processing node id path node was removed or created so let's try again
        LOG_DEBUG(log, "Retrying setProcessing because processing node id path is unexpectedly missing or was created (error code: {})", code);
    }

    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "Failed to set file processing within {} retries, last error: {}",
        max_num_tries, code);
}

void ObjectStorageQueueOrderedFileMetadata::prepareProcessedAtStartRequests(Coordination::Requests & requests)
{
    if (useBucketsForProcessing())
    {
        for (size_t i = 0; i < buckets_num; ++i)
        {
            auto path = getProcessedPathWithBucket(zk_path, i);
            doPrepareProcessedRequests(requests, path, /* ignore_if_exists */true);
        }
    }
    else
    {
        doPrepareProcessedRequests(requests, processed_node_path, /* ignore_if_exists */true);
    }
}

void ObjectStorageQueueOrderedFileMetadata::doPrepareProcessedRequests(
    Coordination::Requests & requests,
    const std::string & processed_node_path_,
    bool ignore_if_exists)
{
    NodeMetadata processed_node;
    Coordination::Stat processed_node_stat;
    if (getMaxProcessedFile(processed_node, &processed_node_stat, processed_node_path_, log))
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

    if (created_processing_node)
        requests.push_back(zkutil::makeRemoveRequest(processing_node_path, -1));
}

void ObjectStorageQueueOrderedFileMetadata::prepareProcessedRequestsImpl(Coordination::Requests & requests)
{
    chassert(created_processing_node);
    doPrepareProcessedRequests(requests, processed_node_path, /* ignore_if_exists */false);
}

void ObjectStorageQueueOrderedFileMetadata::migrateToBuckets(const std::string & zk_path, size_t value, size_t prev_value)
{
    const auto log = getLogger("ObjectStorageQueueOrderedFileMetadata");
    auto zk_client = ObjectStorageQueueMetadata::getZooKeeper(log);
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
            log);

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
                zk_client = ObjectStorageQueueMetadata::getZooKeeper(log);
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
    const bool use_buckets_for_processing = buckets_num > 1;

    buckets_num = std::max<size_t>(buckets_num, 1);
    std::map<size_t, std::string> max_processed_file_per_bucket;

    for (size_t i = 0; i < buckets_num; ++i)
    {
        auto processed_node_path = use_buckets_for_processing
            ? getProcessedPathWithBucket(zk_path_, i)
            : getProcessedPathWithoutBucket(zk_path_);

        NodeMetadata max_processed_file;
        if (getMaxProcessedFile(max_processed_file, {}, processed_node_path, log_))
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

    zkutil::ZooKeeper::MultiTryGetResponse responses;
    ObjectStorageQueueMetadata::getKeeperRetriesControl(log_).retryLoop([&]
    {
        responses = ObjectStorageQueueMetadata::getZooKeeper(log_)->tryGet(failed_paths);
    });
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
