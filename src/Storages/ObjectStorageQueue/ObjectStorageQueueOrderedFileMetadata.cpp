#include <Storages/ObjectStorageQueue/ObjectStorageQueueOrderedFileMetadata.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueMetadata.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>
#include <Poco/JSON/Parser.h>
#include <numeric>

#include <boost/algorithm/string/replace.hpp>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
    extern const int UNEXPECTED_ZOOKEEPER_ERROR;
}

namespace
{
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

    /// Normalize hive part to use as node in zookeeper path
    /// `date=2025-01-01/city=New_Orlean` changes to `date=2025-01-01_city=New__Orlean`
    void normalizeHivePart(std::string & hive_part)
    {
        boost::replace_all(hive_part, "_", "__");
        boost::replace_all(hive_part, "/", "_");
    }

    /// Helper for HIVE mode: return hive part of path
    /// For path `/table/path/date=2026-01-01/city=New_Orlean/data.parquet` returns `date=2026-01-01/city=New_Orlean`
    std::string getHivePart(const std::string & file_path)
    {
        std::string hive_part(VirtualColumnUtils::findHivePartitioningInPath(file_path));
        normalizeHivePart(hive_part);
        return hive_part;
    }

    /// Utility function to extract partition key from file path
    std::string getPartitionKey(
        const std::string & file_path,
        ObjectStorageQueuePartitioningMode partitioning_mode,
        const ObjectStorageQueueFilenameParser * parser)
    {
        switch (partitioning_mode)
        {
            case ObjectStorageQueuePartitioningMode::HIVE:
                return getHivePart(file_path);
            case ObjectStorageQueuePartitioningMode::REGEX: {
                if (parser && parser->isValid())
                {
                    if (auto partition_key = parser->parse(file_path))
                        return *partition_key;
                }
                return "";
            }
            case ObjectStorageQueuePartitioningMode::NONE:
                return "";
        }
    }

    bool hasPartitioningMode(const ObjectStorageQueuePartitioningMode partitioning_mode)
    {
        return partitioning_mode != ObjectStorageQueuePartitioningMode::NONE;
    }

    ObjectStorageQueueOrderedFileMetadata::Bucket getBucketForPathImpl(
        const std::string & path,
        size_t buckets_num,
        ObjectStorageQueueBucketingMode bucketing_mode,
        ObjectStorageQueuePartitioningMode partitioning_mode,
        const ObjectStorageQueueFilenameParser * parser)
    {
        if (!buckets_num)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Buckets number cannot be zero");

        /// Use partition key for bucketing when bucketing_mode is PARTITION
        /// This ensures files from the same partition always go to the same bucket
        if (bucketing_mode == ObjectStorageQueueBucketingMode::PARTITION)
        {
            auto partition_key = getPartitionKey(path, partitioning_mode, parser);
            return sipHash64(partition_key) % buckets_num;
        }

        /// Default hash the full file path
        return sipHash64(path) % buckets_num;
    }

    std::string getProcessedPath(
        const std::filesystem::path & zk_path,
        const std::string & path,
        size_t buckets_num,
        ObjectStorageQueueBucketingMode bucketing_mode,
        ObjectStorageQueuePartitioningMode partitioning_mode,
        const ObjectStorageQueueFilenameParser * parser)
    {
        if (useBucketsForProcessing(buckets_num))
            return getProcessedPathWithBucket(zk_path, getBucketForPathImpl(path, buckets_num, bucketing_mode, partitioning_mode, parser));
        return getProcessedPathWithoutBucket(zk_path);
    }

}

std::string ObjectStorageQueueOrderedFileMetadata::BucketInfo::toString() const
{
    WriteBufferFromOwnString wb;
    wb << "bucket " << bucket << ", ";
    wb << "processor info " << processor_info;
    return wb.str();
}

ObjectStorageQueueOrderedFileMetadata::BucketHolder::BucketHolder(
    const Bucket & bucket_,
    const std::string & bucket_lock_path_,
    const std::string & processor_info_,
    LoggerPtr log_,
    const std::string & zookeeper_name_)
    : bucket_info(std::make_shared<BucketInfo>(BucketInfo{
        .bucket = bucket_,
        .bucket_lock_path = bucket_lock_path_,
        .processor_info = processor_info_,
        .zookeeper_name = zookeeper_name_ }))
    , log(log_)
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    ObjectStorageQueueMetadata::getKeeperRetriesControl(log).retryLoop([&]
    {
        chassert(checkBucketOwnership(ObjectStorageQueueMetadata::getZooKeeper(log, bucket_info->zookeeper_name)));
    });
#endif
}

bool ObjectStorageQueueOrderedFileMetadata::BucketHolder::checkBucketOwnership(std::shared_ptr<ZooKeeperWithFaultInjection> zk_client)
{
    auto processor_info = getProcessorInfo(zk_client);
    if (!processor_info.has_value())
        return false;

    LOG_TEST(
        log, "Bucket lock node {} has owner: {}, current owner: {}",
        bucket_info->bucket_lock_path, processor_info.value(), bucket_info->processor_info);

    return processor_info.value() == bucket_info->processor_info;
}

std::optional<std::string> ObjectStorageQueueOrderedFileMetadata::BucketHolder::getProcessorInfo(std::shared_ptr<ZooKeeperWithFaultInjection> zk_client)
{
    std::string data;
    /// No retries, because they must be done on a higher level.
    if (zk_client->tryGet(bucket_info->bucket_lock_path, data))
        return data;
    return std::nullopt;
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
        auto zk_client = ObjectStorageQueueMetadata::getZooKeeper(log, bucket_info->zookeeper_name);
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

    auto component_guard = Coordination::setCurrentComponent("ObjectStorageQueueOrderedFileMetadata::~BucketHolder");
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
    const std::string & zookeeper_name_,
    ObjectStorageQueueBucketingMode bucketing_mode_,
    ObjectStorageQueuePartitioningMode partitioning_mode_,
    const ObjectStorageQueueFilenameParser * parser_,
    LoggerPtr log_)
    : ObjectStorageQueueIFileMetadata(
        path_,
        zookeeper_name_,
        /* processing_node_path */zk_path_ / "processing" / getNodeName(path_),
        /* processed_node_path */getProcessedPath(zk_path_, path_, buckets_num_, bucketing_mode_, partitioning_mode_, parser_),
        /* failed_node_path */zk_path_ / "failed" / getNodeName(path_),
        file_status_,
        max_loading_retries_,
        metadata_ref_count_,
        use_persistent_processing_nodes_,
        log_)
    , buckets_num(buckets_num_)
    , zk_path(zk_path_)
    , bucket_info(bucket_info_)
    , partitioning_mode(partitioning_mode_)
    , parser(parser_)
{
    LOG_TEST(log, "Path: {}, node_name: {}, max_loading_retries: {}, "
             "processed_path: {}, processing_path: {}, failed_path: {}, partitioning_mode: {}",
             path, node_name, max_loading_retries,
             processed_node_path, processing_node_path, failed_node_path, magic_enum::enum_name(partitioning_mode));
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

bool ObjectStorageQueueOrderedFileMetadata::getMaxProcessedNode(
    NodeMetadata & result,
    Coordination::Stat * stat,
    const std::string & processed_node_path_,
    LoggerPtr log_,
    const std::string & zookeeper_name_)
{
    std::string data;
    bool processed_node_exists = false;
    ObjectStorageQueueMetadata::getKeeperRetriesControl(log_).retryLoop([&]
    {
        processed_node_exists = ObjectStorageQueueMetadata::getZooKeeper(log_, zookeeper_name_)->tryGet(processed_node_path_, data, stat);
    });
    if (processed_node_exists)
    {
        if (!data.empty())
            result = NodeMetadata::fromString(data);
        return true;
    }
    return false;
}

ObjectStorageQueueOrderedFileMetadata::ProcessingStateFromKeeper
ObjectStorageQueueOrderedFileMetadata::getProcessingStateFromKeeper(
    Coordination::Stat * processed_node_stat,
    bool check_failed,
    LoggerPtr log_)
{
    return getProcessingStateFromKeeper(
        processed_node_stat,
        processed_node_path,
        path,
        hasPartitioningMode(partitioning_mode)
            ? std::optional<std::string>(std::filesystem::path(processed_node_path) / getPartitionKey(path, partitioning_mode, parser))
            : std::nullopt,
        check_failed ? std::optional<std::string>(failed_node_path) : std::nullopt,
        log_,
        zookeeper_name);
}

ObjectStorageQueueOrderedFileMetadata::ProcessingStateFromKeeper
ObjectStorageQueueOrderedFileMetadata::getProcessingStateFromKeeper(
    Coordination::Stat * processed_node_stat,
    const std::string & processed_node_path_,
    const std::string & file_path,
    std::optional<std::string> processed_node_hive_partitioning_path,
    std::optional<std::string> failed_node_path_,
    LoggerPtr log_,
    const std::string & zookeeper_name_)
{
    /// Processed path has format of
    /// either `prefix/processed` or `prefix/buckets/<bucket_id>/processed`.
    std::vector<std::string> paths = {processed_node_path_};

    /// Failed path has format `prefix/failed/{node_id}`
    if (failed_node_path_.has_value())
        paths.push_back(*failed_node_path_);

    size_t hive_partitioning_index = paths.size();

    /// Processed hive partitioning path has format of
    /// `prefix/processed/<hive_prefix>/processed`
    /// or
    /// `prefix/processed/buckets/<bucket_id>/<hive_prefix>/processed`
    if (processed_node_hive_partitioning_path.has_value())
        paths.push_back(*processed_node_hive_partitioning_path);

    zkutil::ZooKeeper::MultiTryGetResponse responses;
    auto zk_retry = ObjectStorageQueueMetadata::getKeeperRetriesControl(log_);
    zk_retry.retryLoop([&]
    {
        responses = ObjectStorageQueueMetadata::getZooKeeper(log_, zookeeper_name_)->tryGet(paths);
    });

    auto check_code = [](auto code, auto path)
    {
        if (!(code == Coordination::Error::ZOK || code == Coordination::Error::ZNONODE))
            throw zkutil::KeeperException::fromPath(code, path);
    };

    if (responses.size() != paths.size())
    {
        throw Exception(ErrorCodes::UNEXPECTED_ZOOKEEPER_ERROR,
            "Unexpected size of Keeper response, expected {}, got {}",
            paths.size(), responses.size());
    }

    for (size_t i = 0; i < responses.size(); ++i)
        check_code(responses[i].error, paths[i]);

    bool is_failed = failed_node_path_.has_value() ? responses[1].error == Coordination::Error::ZOK : false;

    if (responses[0].data.empty())
        return ProcessingStateFromKeeper(is_failed);

    NodeMetadata result = NodeMetadata::fromString(responses[0].data);
    if (processed_node_stat)
        *processed_node_stat = responses[0].stat;

    std::string last_processed_path;
    if (processed_node_hive_partitioning_path.has_value())
        last_processed_path = responses[hive_partitioning_index].data;
    else
        last_processed_path = result.file_path;

    return ProcessingStateFromKeeper(file_path, last_processed_path, is_failed);
}

ObjectStorageQueueOrderedFileMetadata::ProcessingStateFromKeeper::ProcessingStateFromKeeper(
    const std::string & path,
    const std::string & last_processed_path_,
    bool is_failed_)
    : last_processed_path(last_processed_path_)
    , is_failed(is_failed_)
    , is_processed(path.empty() || last_processed_path_.empty() ? false : path <= last_processed_path_)
{
}

bool ObjectStorageQueueOrderedFileMetadata::getMaxProcessedFilesByHivePartition(
    std::unordered_map<std::string, std::string> & last_processed_path_per_hive_partition,
    const std::string & processed_node_path_,
    LoggerPtr log_,
    const std::string & zookeeper_name_)
{
    Strings hive_partitions;
    Coordination::Error code;
    auto zk_retry = ObjectStorageQueueMetadata::getKeeperRetriesControl(log_);
    zk_retry.retryLoop([&]
    {
        code = ObjectStorageQueueMetadata::getZooKeeper(log_, zookeeper_name_)->tryGetChildren(processed_node_path_, hive_partitions);
    });

    if (code == Coordination::Error::ZNONODE)
        return false;
    else if (code != Coordination::Error::ZOK)
        throw zkutil::KeeperException::fromPath(code, processed_node_path_);

    Strings hive_partition_processed_paths;
    for (const auto & hive_partition : hive_partitions)
        hive_partition_processed_paths.push_back(std::filesystem::path(processed_node_path_) / hive_partition);

    zkutil::ZooKeeper::MultiTryGetResponse responses;

    zk_retry.resetFailures();
    zk_retry.retryLoop([&]
    {
        responses = ObjectStorageQueueMetadata::getZooKeeper(log_, zookeeper_name_)->tryGet(hive_partition_processed_paths);
    });

    if (responses.size() != hive_partitions.size())
    {
        throw Exception(
            ErrorCodes::UNEXPECTED_ZOOKEEPER_ERROR,
            "Unexpected size of Keeper response, expected {}, got {}",
            hive_partitions.size(), responses.size());
    }

    for (size_t i = 0; i < hive_partitions.size(); ++i)
    {
        if (responses[i].error == Coordination::Error::ZOK)
        {
            last_processed_path_per_hive_partition[hive_partitions[i]] = responses[i].data;
        }
        else if (responses[i].error != Coordination::Error::ZNONODE)
            throw zkutil::KeeperException::fromPath(responses[i].error, hive_partition_processed_paths[i]);
    }
    return true;
}

ObjectStorageQueueOrderedFileMetadata::Bucket
ObjectStorageQueueOrderedFileMetadata::getBucketForPath(
    const std::string & path_,
    size_t buckets_num,
    ObjectStorageQueueBucketingMode bucketing_mode,
    ObjectStorageQueuePartitioningMode partitioning_mode,
    const ObjectStorageQueueFilenameParser * parser)
{
    return getBucketForPathImpl(path_, buckets_num, bucketing_mode, partitioning_mode, parser);
}

ObjectStorageQueueOrderedFileMetadata::BucketHolderPtr ObjectStorageQueueOrderedFileMetadata::tryAcquireBucket(
    const std::filesystem::path & zk_path,
    const Bucket & bucket,
    bool /*use_persistent_processing_nodes_*/,
    const std::string & zookeeper_name_,
    LoggerPtr log_)
{
    auto zk_retry = ObjectStorageQueueMetadata::getKeeperRetriesControl(log_);
    const auto bucket_path = zk_path / "buckets" / toString(bucket);

#ifdef DEBUG_OR_SANITIZER_BUILD
    bool bucket_exists = false;
    zk_retry.retryLoop([&]
    {
        bucket_exists = ObjectStorageQueueMetadata::getZooKeeper(log_, zookeeper_name_)->exists(bucket_path);
    });
    chassert(bucket_exists);
#endif

    const auto bucket_lock_path = bucket_path / "lock";
    const auto processor_info = getProcessorInfo(generateProcessingID());

    Coordination::Error code;
    zk_retry.resetFailures();
    zk_retry.retryLoop([&]
    {
        auto zk_client = ObjectStorageQueueMetadata::getZooKeeper(log_, zookeeper_name_);
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
            log_,
            zookeeper_name_);
    }

    if (code == Coordination::Error::ZNODEEXISTS)
        return nullptr;

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to set file processing, error: {}", code);
}

std::pair<bool, ObjectStorageQueueIFileMetadata::FileStatus::State> ObjectStorageQueueOrderedFileMetadata::setProcessingImpl()
{
    auto zk_client = ObjectStorageQueueMetadata::getZooKeeper(log, zookeeper_name);
    auto zk_retry = ObjectStorageQueueMetadata::getKeeperRetriesControl(log);

    processor_info = getProcessorInfo(generateProcessingID());

    std::string processed_node_partition_path;
    if (hasPartitioningMode(partitioning_mode))
        processed_node_partition_path = std::filesystem::path(processed_node_path) / getPartitionKey(path, partitioning_mode, parser);

    const size_t max_num_tries = 100;
    Coordination::Error code;
    std::string failed_path;
    for (size_t i = 0; i < max_num_tries; ++i)
    {
        Coordination::Stat processed_node_stat;
        auto state = getProcessingStateFromKeeper(&processed_node_stat, /* check_failed */true, log);

        if (state.is_failed)
        {
            LOG_TEST(log, "File {} is Failed, path {}", path, failed_node_path);
            return {false, FileStatus::State::Failed};
        }

        LOG_TEST(log, "Current max processed file is: {}. Processed node path is: {}",
                 state.last_processed_path.has_value() ? *state.last_processed_path : "",
                 processed_node_path);

        if (state.is_processed)
            return {false, FileStatus::State::Processed};

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
        if (state.last_processed_path.has_value())
            requests.push_back(zkutil::makeCheckRequest(processed_node_path, processed_node_stat.version));
        else
            zkutil::addCheckNotExistsRequest(requests, *zk_client, processed_node_path);

        Coordination::Responses responses;
        zk_retry.resetFailures();
        zk_retry.retryLoop([&]
        {
            auto zk = ObjectStorageQueueMetadata::getZooKeeper(log, zookeeper_name);
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
            return {true, FileStatus::State::None};

        auto has_request_failed = [&](size_t request_index) { return responses[request_index]->error != Coordination::Error::ZOK; };

        auto failed_idx = zkutil::getFailedOpIndex(code, responses);
        failed_path = requests[failed_idx]->getPath();
        LOG_DEBUG(log, "Code: {}, failed idx: {}, failed path: {}", code, failed_idx, failed_path);

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
        LOG_DEBUG(
            log, "Retrying setProcessing because processing node id path is "
            "unexpectedly missing or was not created (error code: {})", code);
    }

    LOG_WARNING(
        log, "Failed to set file processing within {} retries, last error {} for path {}",
        max_num_tries, code, failed_path);

    chassert(false); /// Catch in CI.
    return {false, FileStatus::State::None};
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
    bool ignore_if_exists,
    LastProcessedFileInfoMapPtr created_nodes)
{
    Coordination::Stat processed_node_stat;
    auto state = getProcessingStateFromKeeper(&processed_node_stat, /* check_failed */false, log);

    if (state.last_processed_path.has_value())
    {
        if (state.is_processed)
        {
            LOG_TRACE(
                log, "File {} is already processed, current max processed file: {}",
                path, *state.last_processed_path);

            if (ignore_if_exists)
                return;

            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "File ({}) is already processed, while expected it not to be (path: {})",
                path, processed_node_path_);
        }

        LOG_TEST(
            log, "Current max processed file is: {}. Path {} hasn't been processed yet",
            state.last_processed_path.value(), path);
    }

    Coordination::RequestPtr request;
    if (state.last_processed_path.has_value())
        request = zkutil::makeSetRequest(processed_node_path_, node_metadata.toString(), processed_node_stat.version);
    else
        request = zkutil::makeCreateRequest(processed_node_path_, node_metadata.toString(), zkutil::CreateMode::Persistent);

    if (created_nodes)
    {
        if (auto it = created_nodes->find(processed_node_path_); it != created_nodes->end())
        {
            if (it->second.file_path < path)
            {
                /// Node was added in requests before, which is possible if processing_threads_num > 1
                LOG_TEST(
                    log, "Path {} was already created in this request pack, overridden with {}",
                    processed_node_path_, path);

                it->second.file_path = path;
                requests[it->second.index] = std::move(request);
            }
        }
        else
        {
            created_nodes->emplace(
                processed_node_path_,
                LastProcessedFileInfo({/* last_processed_path */path, /* request_index */requests.size()}));

            LOG_TEST(
                log, "Adding {} request for path {} (processed_node_path: {})",
                state.last_processed_path.has_value() ? "SET" : "CREATE",
                path, processed_node_path_);

            requests.push_back(std::move(request));
        }
    }
    else
    {
        LOG_TEST(
            log, "Adding {} request for path {} (processed_node_path: {})",
            state.last_processed_path.has_value() ? "SET" : "CREATE",
            path, processed_node_path_);

        requests.push_back(std::move(request));
    }

    if (created_processing_node)
        requests.push_back(zkutil::makeRemoveRequest(processing_node_path, -1));
}

void ObjectStorageQueueOrderedFileMetadata::prepareProcessedRequestsImpl(
    Coordination::Requests & requests,
    LastProcessedFileInfoMapPtr created_nodes)
{
    chassert(created_processing_node);
    doPrepareProcessedRequests(requests, processed_node_path, /* ignore_if_exists */false, created_nodes);
}

void ObjectStorageQueueOrderedFileMetadata::preparePartitionProcessedMap(PartitionLastProcessedFileInfoMap & last_processed_file_per_partition)
{
    if (!hasPartitioningMode(partitioning_mode))
        return;

    const auto partition_processed_path = std::filesystem::path(processed_node_path) / getPartitionKey(node_metadata.file_path, partitioning_mode, parser);

    if (auto it = last_processed_file_per_partition.find(partition_processed_path);
        it != last_processed_file_per_partition.end())
    {
        auto & last_processed_path = it->second.file_path;
        if (last_processed_path < node_metadata.file_path)
            last_processed_path = node_metadata.file_path;
    }
    else
    {
        auto zk_retry = ObjectStorageQueueMetadata::getKeeperRetriesControl(log);
        zk_retry.retryLoop([&]
        {
            bool processed_path_exists = ObjectStorageQueueMetadata::getZooKeeper(log, zookeeper_name)->exists(partition_processed_path);
            last_processed_file_per_partition.emplace(
                partition_processed_path,
                PartitionLastProcessedFileInfo({
                    processed_path_exists,
                    /* last_processed_path */node_metadata.file_path}));
        });
    }
}

void ObjectStorageQueueOrderedFileMetadata::migrateToBuckets(
    const std::string & zk_path,
    size_t value,
    size_t prev_value,
    const std::string & zookeeper_name_)
{
    const auto log = getLogger("ObjectStorageQueueOrderedFileMetadata");
    auto zk_client = ObjectStorageQueueMetadata::getZooKeeper(log, zookeeper_name_);
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
        bool has_processed_node = ObjectStorageQueueOrderedFileMetadata::getMaxProcessedNode(
            processed_node,
            &processed_node_stat,
            old_processed_path,
            log,
            zookeeper_name_);

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
                zk_client = ObjectStorageQueueMetadata::getZooKeeper(log, zookeeper_name_);
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
    const std::string & zookeeper_name_,
    ObjectStorageQueueBucketingMode bucketing_mode,
    ObjectStorageQueuePartitioningMode partitioning_mode,
    const ObjectStorageQueueFilenameParser * parser,
    LoggerPtr log_)
{
    const bool use_buckets_for_processing = buckets_num > 1;
    buckets_num = std::max<size_t>(buckets_num, 1);

    /// When partitioning is not used, consider PartitionKey an empty string.
    using PartitionKey = std::string;
    std::map<Bucket, std::unordered_map<PartitionKey, std::string>> last_processed_file_map;

    for (size_t i = 0; i < buckets_num; ++i)
    {
        auto processed_node_path = use_buckets_for_processing
            ? getProcessedPathWithBucket(zk_path_, i)
            : getProcessedPathWithoutBucket(zk_path_);

        if (hasPartitioningMode(partitioning_mode))
        {
            std::unordered_map<PartitionKey, std::string> max_processed_files;
            if (getMaxProcessedFilesByHivePartition(max_processed_files, processed_node_path, log_, zookeeper_name_))
                last_processed_file_map[i] = std::move(max_processed_files);
        }
        else
        {
            auto state = getProcessingStateFromKeeper(
                /* processed_node_stat */{},
                processed_node_path,
                /* file_path */"",
                std::nullopt,
                std::nullopt,
                log_,
                zookeeper_name_);
            if (state.last_processed_path.has_value())
                last_processed_file_map[i][""] = *state.last_processed_path;
        }
    }

    std::vector<std::string> failed_paths;
    std::vector<size_t> check_paths_indexes;
    for (size_t i = 0; i < paths.size(); ++i)
    {
        const auto & path = paths[i];
        const auto bucket = use_buckets_for_processing
            ? getBucketForPathImpl(path, buckets_num, bucketing_mode, partitioning_mode, parser)
            : 0;
        if (!last_processed_file_map.empty())
        {
            if (hasPartitioningMode(partitioning_mode))
            {
                auto partition_key = getPartitionKey(path, partitioning_mode, parser);
                auto max_processed_file = last_processed_file_map[bucket].find(partition_key);
                if (max_processed_file != last_processed_file_map[bucket].end()
                    && path <= max_processed_file->second)
                {
                    LOG_TEST(log_, "Skipping file {}: Processed", path);
                    continue;
                }
            }
            else
            {
                if (path <= last_processed_file_map[bucket][""])
                {
                    LOG_TEST(log_, "Skipping file {}: Processed", path);
                    continue;
                }
            }
        }
        failed_paths.push_back(zk_path_ / "failed" / getNodeName(path));
        check_paths_indexes.push_back(i);
    }

    std::vector<std::string> result;
    if (failed_paths.empty())
    {
        paths.clear();
        return; /// All files are already processed.
    }

    auto check_code = [&](auto code, const std::string & path)
    {
        if (!(code == Coordination::Error::ZOK || code == Coordination::Error::ZNONODE))
            throw zkutil::KeeperException::fromPath(code, path);
    };

    zkutil::ZooKeeper::MultiTryGetResponse responses;
    ObjectStorageQueueMetadata::getKeeperRetriesControl(log_).retryLoop([&]
    {
        responses = ObjectStorageQueueMetadata::getZooKeeper(log_, zookeeper_name_)->tryGet(failed_paths);
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
