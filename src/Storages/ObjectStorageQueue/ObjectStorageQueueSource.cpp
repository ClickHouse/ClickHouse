#include "config.h"

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/FailPoint.h>
#include <Common/CurrentMetrics.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/logger_useful.h>
#include <Common/parseGlobs.h>
#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Context.h>
#include <Interpreters/InsertDeduplication.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueMetadata.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueSource.h>
#include <Storages/ObjectStorageQueue/StorageObjectStorageQueue.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueUnorderedFileMetadata.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueOrderedFileMetadata.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/HivePartitioningUtils.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageIterator.h>
#include <Processors/Executors/PullingPipelineExecutor.h>


namespace ProfileEvents
{
    extern const Event ObjectStorageQueuePullMicroseconds;
    extern const Event ObjectStorageQueueFailedToBatchSetProcessing;
    extern const Event ObjectStorageQueueTrySetProcessingSucceeded;
    extern const Event ObjectStorageQueueListedFiles;
    extern const Event ObjectStorageQueueFilteredFiles;
    extern const Event ObjectStorageQueueReadFiles;
    extern const Event ObjectStorageQueueReadRows;
    extern const Event ObjectStorageQueueReadBytes;
    extern const Event ObjectStorageQueueExceptionsDuringRead;
    extern const Event ObjectStorageQueueExceptionsDuringInsert;
    extern const Event ObjectStorageQueueCancelledFiles;
}

namespace DB
{
namespace Setting
{
    extern const SettingsMaxThreads max_parsing_threads;
    extern const SettingsUInt64 keeper_max_retries;
}

namespace ServerSetting
{
    extern const ServerSettingsInsertDeduplicationVersions insert_deduplication_version;
}

namespace ObjectStorageQueueSetting
{
    extern const ObjectStorageQueueSettingsObjectStorageQueueAction after_processing;
    extern const ObjectStorageQueueSettingsUInt64 max_processed_bytes_before_commit;
    extern const ObjectStorageQueueSettingsUInt64 max_processed_files_before_commit;
    extern const ObjectStorageQueueSettingsUInt64 max_processed_rows_before_commit;
    extern const ObjectStorageQueueSettingsUInt64 max_processing_time_sec_before_commit;
}

namespace FailPoints
{
    extern const char object_storage_queue_fail_in_the_middle_of_file[];
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_COMPILE_REGEXP;
    extern const int UNKNOWN_EXCEPTION;
    extern const int TOO_MANY_PARTS;
    extern const int TABLE_IS_READ_ONLY;
    extern const int TABLE_IS_BEING_RESTARTED;
}

ObjectStorageQueueSource::ObjectStorageQueueObjectInfo::ObjectStorageQueueObjectInfo(
    const ObjectInfo & object_info, ObjectStorageQueueMetadata::FileMetadataPtr file_metadata_)
    : ObjectInfo(RelativePathWithMetadata{object_info.getPath(), object_info.getObjectMetadata()})
    , file_metadata(file_metadata_)
{
}

ObjectStorageQueueSource::FileIterator::FileIterator(
    std::shared_ptr<ObjectStorageQueueMetadata> metadata_,
    ObjectStoragePtr object_storage_,
    StorageObjectStorageConfigurationPtr configuration_,
    const StorageID & storage_id_,
    size_t list_objects_batch_size_,
    const ActionsDAG::Node * predicate_,
    const NamesAndTypesList & virtual_columns_,
    const NamesAndTypesList & hive_partition_columns_to_read_from_file_path_,
    ContextPtr context_,
    LoggerPtr logger_,
    bool enable_hash_ring_filtering_,
    bool file_deletion_on_processed_enabled_,
    std::atomic<bool> & shutdown_called_)
    : WithContext(context_)
    , metadata(metadata_)
    , object_storage(object_storage_)
    , configuration(configuration_)
    , virtual_columns(virtual_columns_)
    , hive_partition_columns_to_read_from_file_path(hive_partition_columns_to_read_from_file_path_)
    , file_deletion_on_processed_enabled(file_deletion_on_processed_enabled_)
    , mode(metadata->getTableMetadata().getMode())
    , enable_hash_ring_filtering(enable_hash_ring_filtering_)
    , storage_id(storage_id_)
    , use_buckets_for_processing(metadata->useBucketsForProcessing())
    , buckets_num(use_buckets_for_processing ? metadata->getBucketsNum() : 0)
    , shutdown_called(shutdown_called_)
    , log(logger_)
{
    if (configuration->isNamespaceWithGlobs())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expression can not have wildcards inside namespace name");

    const auto & reading_path = configuration->getPathForRead();

    if (!reading_path.hasGlobs())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Using glob iterator with path without globs is not allowed (used path: {})",
            reading_path.path);
    }

    const auto globbed_key = reading_path.path;
    object_storage_iterator = object_storage->iterate(
        reading_path.cutGlobs(configuration->supportsPartialPathPrefix()),
        list_objects_batch_size_,
        /*with_tags=*/ false);

    matcher = std::make_unique<re2::RE2>(makeRegexpPatternFromGlobs(globbed_key));
    if (!matcher->ok())
    {
        throw Exception(
            ErrorCodes::CANNOT_COMPILE_REGEXP,
            "Cannot compile regex from glob ({}): {}",
            globbed_key, matcher->error());
    }

    recursive = globbed_key == "/**";
    if (auto filter_dag = VirtualColumnUtils::createPathAndFileFilterDAG(predicate_, virtual_columns, context_))
    {
        VirtualColumnUtils::buildSetsForDAG(*filter_dag, context_);
        filter_expr = std::make_shared<ExpressionActions>(std::move(*filter_dag));
    }

    if (use_buckets_for_processing)
    {
        if (!buckets_num)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Buckets number cannot be zero");

        for (size_t i = 0; i < buckets_num; ++i)
            keys_cache_per_bucket.emplace(i, std::make_unique<BucketInfo>());
    }
}

bool ObjectStorageQueueSource::FileIterator::isFinished()
{
    std::lock_guard lock(mutex);
    LOG_TEST(log, "Iterator finished: {}, objects to retry: {}", iterator_finished.load(), objects_to_retry.size());
    return iterator_finished
        && std::all_of(keys_cache_per_bucket.begin(), keys_cache_per_bucket.end(), [](const auto & v) { return v.second->keys.empty(); })
        && objects_to_retry.empty();
}

size_t ObjectStorageQueueSource::FileIterator::estimatedKeysCount()
{
    std::lock_guard lock(next_mutex);
    /// Copied from StorageObjectStorageSource::estimateKeysCount().
    if (object_infos.empty() && !is_finished && object_storage_iterator->isValid())
        return std::numeric_limits<size_t>::max();
    else
        return object_infos.size();
}

std::pair<ObjectInfoPtr, ObjectStorageQueueSource::FileMetadataPtr>
ObjectStorageQueueSource::FileIterator::next()
{
    std::lock_guard lock(next_mutex);

    bool current_batch_processed = object_infos.empty() || index >= object_infos.size();
    if (is_finished && current_batch_processed)
    {
        LOG_TEST(log, "is_finished: {}, index: {}, object_infos.size(): {}",
                 is_finished, index, object_infos.size());
        return {};
    }

    auto zk_retry = ObjectStorageQueueMetadata::getKeeperRetriesControl(log);
    if (current_batch_processed)
    {
        file_metadatas.clear();
        Stopwatch get_object_watch;
        ObjectInfos new_batch;

        while (new_batch.empty())
        {
            auto result = object_storage_iterator->getCurrentBatchAndScheduleNext();
            if (!result.has_value())
            {
                is_finished = true;
                return {};
            }

            LOG_TEST(log, "Received batch of size: {}", result->size());

            std::transform(
                result->begin(),
                result->end(),
                std::back_inserter(new_batch),
                [&](const std::shared_ptr<RelativePathWithMetadata> & object) { return std::make_shared<ObjectInfo>(*object); });
            ProfileEvents::increment(ProfileEvents::ObjectStorageQueueListedFiles, new_batch.size());

            for (auto it = new_batch.begin(); it != new_batch.end();)
            {
                if (!recursive && !re2::RE2::FullMatch((*it)->getPath(), *matcher))
                    it = new_batch.erase(it);
                else
                    ++it;
            }

            if (filter_expr)
            {
                std::vector<String> paths;
                paths.reserve(new_batch.size());
                for (const auto & object_info : new_batch)
                    paths.push_back(Source::getUniqueStoragePathIdentifier(*configuration, *object_info, false));

                /// Hive partition columns were not being used in ObjectStorageQueue before the refactoring from (virtual -> physical).
                /// So we are keeping it the way it is for now
                VirtualColumnUtils::filterByPathOrFile(
                    new_batch, paths, filter_expr, virtual_columns, hive_partition_columns_to_read_from_file_path, getContext());

                LOG_TEST(log, "Filtered files: {} -> {} by path or filename", paths.size(), new_batch.size());
            }

            size_t previous_size = new_batch.size();

            /// Filter out files which we know we would not need to process.
            filterProcessableFiles(new_batch);

            LOG_TEST(log, "Filtered processed and failed files: {} -> {}", previous_size, new_batch.size());

            if (!new_batch.empty()
                && enable_hash_ring_filtering
                && mode == ObjectStorageQueueMode::UNORDERED)
            {
                file_metadatas.resize(new_batch.size());

                std::vector<ObjectStorageQueueIFileMetadata::SetProcessingResponseIndexes> result_indexes;
                result_indexes.resize(new_batch.size());

                Coordination::Requests requests;
                size_t num_successful_objects = 0;
                Strings processing_paths;
                String processor_info;
                const auto processing_id = ObjectStorageQueueIFileMetadata::generateProcessingID();
                for (size_t i = 0; i < new_batch.size(); ++i)
                {
                    file_metadatas[i] = metadata->getFileMetadata(
                        new_batch[i]->getPath(),
                        /* bucket_info */ {}); /// No buckets for Unordered mode.

                    auto set_processing_result = file_metadatas[i]->prepareSetProcessingRequests(requests, processing_id);
                    if (set_processing_result.has_value())
                    {
                        result_indexes[i] = set_processing_result.value();
                        ++num_successful_objects;
                        processing_paths.push_back(file_metadatas[i]->getProcessingPath());

                        const auto & current_processor_info = file_metadatas[i]->getProcessorInfo();
                        if (processor_info.empty())
                            processor_info = current_processor_info;

                        chassert(processor_info == current_processor_info,
                                 fmt::format("{} != {}", processor_info, current_processor_info));
                    }
                    else
                    {
                        new_batch[i] = nullptr;
                        file_metadatas[i] = nullptr;
                    }
                }

                Coordination::Responses responses;
                Coordination::Error code;
                zk_retry.retryLoop([&]
                {
                    auto zk_client = metadata->getZooKeeper();
                    if (zk_retry.isRetry())
                    {
                        LOG_TEST(log, "Retrying set processing requests batch ({})", processing_paths.size());

                        bool failed = false;
                        bool is_multi_read_enabled = zk_client->isFeatureEnabled(DB::KeeperFeatureFlag::MULTI_READ);
                        if (is_multi_read_enabled)
                        {
                            auto processing_paths_responses = metadata->getZooKeeper()->tryGet(processing_paths);
                            for (size_t i = 0; i < processing_paths_responses.size(); ++i)
                            {
                                LOG_TEST(log, "Path {} has processor: {}, current processor: {}",
                                         processing_paths[i], processing_paths_responses[i].data, processor_info);

                                if (processing_paths_responses[i].data != processor_info)
                                {
                                    failed = true;
                                    break;
                                }
                            }
                        }
                        else
                        {
                            auto processing_paths_responses = zk_client->tryGet(processing_paths);
                            for (size_t i = 0; i < processing_paths_responses.size(); ++i)
                            {
                                const auto & response = processing_paths_responses[i];
                                if (response.error == Coordination::Error::ZNONODE)
                                {
                                    LOG_TEST(log, "Path {} does not exist", processing_paths[i]);
                                    failed = true;
                                    break;
                                }
                                if (response.error == Coordination::Error::ZOK)
                                {
                                    LOG_TEST(log, "Having {}, current processor: {}", response.data, processor_info);
                                    if (response.data != processor_info)
                                    {
                                        failed = true;
                                        break;
                                    }
                                }
                                else
                                {
                                    LOG_WARNING(log, "Unexpected error: {}, path: {}", response.error, processing_paths[i]);
                                    chassert(false);
                                    failed = true;
                                    break;
                                }
                            }
                        }

                        if (!failed)
                        {
                            LOG_TEST(log, "Operation succeeded");
                            code = Coordination::Error::ZOK;
                            return;
                        }
                    }
                    code = zk_client->tryMulti(requests, responses);
                });

                if (code == Coordination::Error::ZOK)
                {
                    ProfileEvents::increment(ProfileEvents::ObjectStorageQueueTrySetProcessingSucceeded, num_successful_objects);

                    LOG_TEST(log, "Successfully set {} files as processing", new_batch.size());

                    for (size_t i = 0; i < new_batch.size(); ++i)
                    {
                        if (!new_batch[i])
                            continue;

                        file_metadatas[i]->afterSetProcessing(/* success */true, std::nullopt);
                    }
                }
                else
                {
                    ProfileEvents::increment(ProfileEvents::ObjectStorageQueueFailedToBatchSetProcessing);

                    auto failed_idx = zkutil::getFailedOpIndex(code, responses);

                    LOG_TRACE(log, "Failed to set files as processing in one request: {} ({})",
                              code, requests[failed_idx]->getPath());

                    file_metadatas.clear();
                }

                if (num_successful_objects != new_batch.size())
                {
                    size_t batch_i = 0;
                    for (size_t i = 0; i < num_successful_objects; ++i, ++batch_i)
                    {
                        while (batch_i < new_batch.size() && !new_batch[batch_i])
                            ++batch_i;

                        if (batch_i == new_batch.size())
                        {
                            throw Exception(
                                ErrorCodes::LOGICAL_ERROR,
                                "Mismatch num_successful_objects ({}) is less than the number of valid objects",
                                num_successful_objects);
                        }

                        new_batch[i] = new_batch[batch_i];
                        file_metadatas[i] = file_metadatas[batch_i];
                    }
                    new_batch.resize(num_successful_objects);
                    file_metadatas.resize(num_successful_objects);
                }

                chassert(file_metadatas.empty() || new_batch.size() == file_metadatas.size());
            }

            if (!new_batch.empty())
            {
                UInt64 get_object_time_ms = get_object_watch.elapsedMilliseconds();
                for (const auto & file_metadata : file_metadatas)
                    file_metadata->getFileStatus()->setGetObjectTime(get_object_time_ms);
            }
        }

        index = 0;
        object_infos = std::move(new_batch);

        ProfileEvents::increment(ProfileEvents::ObjectStorageQueueFilteredFiles, object_infos.size());
    }

    if (index >= object_infos.size())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Index out of bound for blob metadata. Index: {}, size: {}",
            index, object_infos.size());
    }

    auto result = std::make_pair(
        object_infos[index],
        file_metadatas.empty() ? nullptr : file_metadatas[index]);

    if (result.second && result.first->getPath() != result.second->getPath())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Mismatch {} and {}", result.first->getPath(), result.second->getPath());
    }

    ++index;
    return result;
}

void ObjectStorageQueueSource::FileIterator::filterProcessableFiles(ObjectInfos & objects)
{
    std::vector<std::string> paths;
    paths.reserve(objects.size());
    for (const auto & object : objects)
        paths.push_back(object->getPath());

    if (enable_hash_ring_filtering && mode == ObjectStorageQueueMode::UNORDERED)
        metadata->filterOutForProcessor(paths, storage_id);

    const auto & zookeeper_name = metadata->getZooKeeperName();
    if (mode == ObjectStorageQueueMode::UNORDERED)
        ObjectStorageQueueUnorderedFileMetadata::filterOutProcessedAndFailed(paths, metadata->getPath(), zookeeper_name, log);
    else
        ObjectStorageQueueOrderedFileMetadata::filterOutProcessedAndFailed(
            paths,
            metadata->getPath(),
            metadata->getBucketsNum(),
            zookeeper_name,
            metadata->getBucketingMode(),
            metadata->getPartitioningMode(),
            metadata->getFilenameParser(),
            log);

    std::unordered_set<std::string> paths_set;
    std::ranges::move(paths, std::inserter(paths_set, paths_set.end()));

    ObjectInfos result;
    result.reserve(paths_set.size());
    for (auto & object : objects)
    {
        if (paths_set.contains(object->getPath()))
            result.push_back(std::move(object));
    }
    objects = std::move(result);
}

ObjectInfoPtr ObjectStorageQueueSource::FileIterator::next(size_t processor)
{
    while (!shutdown_called)
    {
        FileMetadataPtr file_metadata;
        ObjectInfoPtr object_info;
        ObjectStorageQueueOrderedFileMetadata::BucketInfoPtr bucket_info;

        chassert(
            use_buckets_for_processing == metadata->useBucketsForProcessing(),
            fmt::format("Current buckets: {}, expected: {}", metadata->getBucketsNum(), buckets_num));

        if (use_buckets_for_processing)
        {
            std::lock_guard lock(mutex);
            auto result = getNextKeyFromAcquiredBucket(processor);
            object_info = result.object_info;
            file_metadata = result.file_metadata;
            bucket_info = result.bucket_info;
        }
        else
        {
            std::lock_guard lock(mutex);
            if (objects_to_retry.empty())
            {
                std::tie(object_info, file_metadata) = next();
                if (!object_info)
                    iterator_finished = true;
            }
            else
            {
                std::tie(object_info, file_metadata) = objects_to_retry.front();
                objects_to_retry.pop_front();
            }
        }

        if (!object_info)
        {
            LOG_TEST(log, "No object left");
            return {};
        }

        if (shutdown_called)
        {
            LOG_DEBUG(log, "Shutdown was called, stopping file iterator");
            return {};
        }

        if (!file_metadata)
        {
            file_metadata = metadata->getFileMetadata(object_info->getPath(), bucket_info);
            if (!file_metadata->trySetProcessing())
                continue;
        }

        if (file_deletion_on_processed_enabled && !object_storage->exists(StoredObject(object_info->getPath())))
        {
            /// Imagine the following case:
            /// Replica A processed fileA and deletes it afterwards.
            /// Replica B has a list request batch (by default list batch is 1000 elements)
            /// and this batch was collected from object storage before replica A processed fileA.
            /// fileA could be somewhere in the middle of this batch of replica B
            /// and replica A processed it before replica B reached fileA in this batch.
            /// All would be alright, unless user has tracked_files_size_limit or tracked_files_ttl_limit
            /// which could expire before replica B reached fileA in this list batch.
            /// It would mean that replica B listed this file while it no longer
            /// exists in object storage at the moment it wants to process it, but
            /// because of tracked_files_size(ttl)_limit expiration - we no longer
            /// have information in keeper that the file was actually processed before,
            /// so replica B would successfully set itself as processor of this file in keeper
            /// and face "The specified key does not exist" after that.
            ///
            /// This existence check here is enough,
            /// only because we do applyActionAfterProcessing BEFORE setting file as processed
            /// and because at this exact place we already successfully set file as processing,
            /// e.g. file deletion and marking file as processed in keeper already took place.
            ///
            /// Note: this all applies only for Unordered mode.
            LOG_TRACE(log, "Ignoring {} because of the race with list & delete", object_info->getPath());

            file_metadata->resetProcessing();
            continue;
        }

        return std::make_shared<ObjectStorageQueueObjectInfo>(*object_info, std::move(file_metadata));
    }
    return {};
}

void ObjectStorageQueueSource::FileIterator::returnForRetry(ObjectInfoPtr object_info, FileMetadataPtr file_metadata)
{
    chassert(object_info);
    chassert(
        use_buckets_for_processing == metadata->useBucketsForProcessing(),
        fmt::format("Current buckets: {}, expected: {}", metadata->getBucketsNum(), buckets_num));

    if (use_buckets_for_processing)
    {
        const auto bucket = ObjectStorageQueueMetadata::getBucketForPath(
            object_info->getPath(),
            buckets_num,
            metadata->getBucketingMode(),
            metadata->getPartitioningMode(),
            metadata->getFilenameParser());
        std::lock_guard lock(mutex);
        keys_cache_per_bucket.at(bucket)->keys.emplace_front(object_info, file_metadata);
    }
    else
    {
        std::lock_guard lock(mutex);
        objects_to_retry.emplace_back(object_info, file_metadata);
    }
}

void ObjectStorageQueueSource::FileIterator::releaseFinishedBuckets()
{
    std::lock_guard lock(mutex);
    for (auto & [processor, holders] : bucket_holders)
    {
        std::string buckets_str;
        size_t released_holders = 0;
        for (auto it = holders->begin(); it != holders->end();)
        {
            const auto & holder = *it;
            const auto bucket = holder->getBucketInfo()->bucket;
            /// Only the last holder in the list of holders can be non-finished.
            if (std::next(it) == holders->end())
            {
                if (!holder->isFinished())
                {
                    /// Do not release non-finished bucket holder. We will continue processing it.
                    LOG_TEST(log, "Bucket {} is not finished yet, will not release it", bucket);
                    break;
                }
            }
            else
                chassert(holder->isFinished());

            /// Release bucket lock.
            holder->release();
            ++released_holders;

            /// Reset bucket processor in cached state.
            auto cached_info = keys_cache_per_bucket.find(bucket);
            if (cached_info != keys_cache_per_bucket.end())
                cached_info->second->processor.reset();

            if (!buckets_str.empty())
                buckets_str += ", ";
            buckets_str += toString(bucket);

            it = holders->erase(it);
        }
        LOG_TRACE(log, "Released {} bucket holders for processor {} "
                  "(released buckets: {}, remaining holders: {}, remaining bucket: {})",
                  released_holders, processor, buckets_str,
                  holders->size(), holders->empty() ? "" : toString(holders->front()->getBucketInfo()->bucket));
    }
}

ObjectStorageQueueSource::BucketHolderPtr ObjectStorageQueueSource::FileIterator::tryAcquireBucket(
    size_t bucket,
    BucketInfo & bucket_info,
    BucketHolders & acquired_buckets,
    size_t processor) const
{
    if (bucket_info.processor.has_value())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Bucket already has a processor: {} (attempted processor: {})",
            bucket_info.processor.value(), processor);
    }

    auto holder = metadata->tryAcquireBucket(bucket);
    if (!holder)
    {
        LOG_TEST(log, "Bucket {} is already locked for processing (keys: {})", bucket, bucket_info.keys.size());
        return nullptr;
    }

    acquired_buckets.push_back(holder);
    bucket_info.processor = processor;

    LOG_TRACE(log, "Processor {} acquired bucket: {} (keys: {})", processor, bucket, bucket_info.keys.size());
    return holder;
}

std::string ObjectStorageQueueSource::FileIterator::bucketHoldersToString() const
{
    std::string processors_infos;
    for (const auto & [processor, bucket_holder] : bucket_holders)
    {
        if (!processors_infos.empty())
            processors_infos += ", ";

        processors_infos += fmt::format("processor {} -> {} buckets ", processor, bucket_holder->size());
        if (!bucket_holder->empty())
        {
            processors_infos += "(";
            for (const auto & bucket : *bucket_holder)
                processors_infos += toString(bucket->getBucket()) + " ";
            processors_infos += ")";
        }
    }
    return processors_infos;
}

ObjectStorageQueueSource::FileIterator::NextKeyFromBucket
ObjectStorageQueueSource::FileIterator::getNextKeyFromAcquiredBucket(size_t processor)
{
    std::shared_ptr<BucketHolders> acquired_buckets = [this, processor]() TSA_REQUIRES(mutex)
    {
        auto it = bucket_holders.find(processor);
        if (it == bucket_holders.end())
            it = bucket_holders.emplace(processor, std::make_shared<BucketHolders>()).first;
        return it->second;
    }();

    BucketHolderPtr current_bucket_holder = acquired_buckets->empty() || acquired_buckets->back()->isFinished()
        ? nullptr
        : acquired_buckets->back();

#ifdef DEBUG_OR_SANITIZER_BUILD
    if (current_bucket_holder)
    {
        ObjectStorageQueueMetadata::getKeeperRetriesControl(log).retryLoop([&]
        {
            auto zk_client = metadata->getZooKeeper();
            chassert(current_bucket_holder->checkBucketOwnership(zk_client));
        });
    }
#endif

    LOG_TEST(
        log, "Current processor: {}, acquired bucket: {}",
        processor, current_bucket_holder ? toString(current_bucket_holder->getBucket()) : "None");

    while (!shutdown_called)
    {
        /// Each processing thread gets next path
        /// and checks if corresponding bucket is already acquired by someone.
        /// In case it is already acquired, they put the key into keys_cache_per_bucket,
        /// so that the thread who acquired the bucket will be able to see
        /// those keys without the need to list s3 directory once again.
        if (current_bucket_holder)
        {
            const auto bucket = current_bucket_holder->getBucket();
            auto it = keys_cache_per_bucket.find(bucket);
            if (it != keys_cache_per_bucket.end())
            {
                /// `bucket_keys` -- keys we iterated so far and which were not taken for processing.
                /// `bucket_processor` -- processor id of the thread which has acquired the bucket.
                auto & [bucket_keys, bucket_processor] = *it->second;

                /// Check correctness just in case.
                if (!bucket_processor.has_value())
                {
                    LOG_TRACE(log, "Set processor {} for bucket {}", processor, bucket);
                    bucket_processor = processor;
                }
                else if (bucket_processor.value() != processor)
                {
                    std::optional<std::string> processor_info;
                    ObjectStorageQueueMetadata::getKeeperRetriesControl(log).retryLoop([&]
                    {
                        auto zk_client = metadata->getZooKeeper();
                        processor_info = current_bucket_holder->getProcessorInfo(zk_client);
                    });
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Expected current processor {} to be equal to {} for bucket {} "
                        "(current bucket: {}, owner: {}, bucket holders: {})",
                        processor,
                        bucket_processor.has_value() ? toString(bucket_processor.value()) : "None",
                        bucket,
                        current_bucket_holder->getBucketInfo()->toString(),
                        processor_info.value_or("None"), bucketHoldersToString());
                }

                if (!bucket_keys.empty())
                {
                    /// Take the key from the front, the order is important.
                    auto [object_info, file_metadata] = bucket_keys.front();
                    bucket_keys.pop_front();

                    LOG_TEST(log, "Current bucket: {}, will process file: {}", bucket, object_info->getFileName());

                    return {object_info, file_metadata, current_bucket_holder->getBucketInfo()};
                }
            }

            if (iterator_finished)
            {
                /// Bucket is fully processed, but we will release it later
                /// - once we write and commit files via commit() method.
                current_bucket_holder->setFinished();
                current_bucket_holder = nullptr;
            }
        }

        /// If processing thread has already acquired some bucket
        /// and while listing object storage directory gets a key which is in a different bucket,
        /// it puts the key into keys_cache_per_bucket to allow others to process it,
        /// because one processing thread can acquire only one bucket at a time.
        /// Once a thread is finished with its acquired bucket, it checks keys_cache_per_bucket
        /// to see if there are keys from buckets not acquired by anyone.
        if (!current_bucket_holder)
        {
            LOG_TEST(log, "Checking caches keys: {}", keys_cache_per_bucket.size());

            for (auto it = keys_cache_per_bucket.begin(); it != keys_cache_per_bucket.end();)
            {
                auto & [bucket, bucket_info] = *it;

                LOG_TEST(log, "Bucket: {}, cached keys: {}, processor: {}",
                         bucket, bucket_info->keys.size(),
                         bucket_info->processor.has_value() ? toString(bucket_info->processor.value()) : "None");

                if (bucket_info->processor.has_value())
                {
                    LOG_TEST(log, "Bucket {} is already locked for processing by {} (keys: {})",
                             bucket, bucket_info->processor.value(), bucket_info->keys.size());
                    ++it;
                    continue;
                }

                if (bucket_info->keys.empty())
                {
                    ++it;
                    continue;
                }

                current_bucket_holder = tryAcquireBucket(bucket, *bucket_info, *acquired_buckets, processor);
                if (!current_bucket_holder)
                {
                    LOG_TEST(log, "Bucket {} is already locked for processing (keys: {})",
                             bucket, bucket_info->keys.size());
                    ++it;
                    continue;
                }

                /// Take the key from the front, the order is important.
                auto [object_info, file_metadata] = bucket_info->keys.front();
                bucket_info->keys.pop_front();

                return {object_info, file_metadata, current_bucket_holder->getBucketInfo()};
            }
        }

        if (iterator_finished)
        {
            LOG_TEST(log, "Reached the end of file iterator and nothing left in keys cache");
            return {};
        }

        while (true)
        {
            auto [object_info, file_metadata] = next();
            if (!object_info)
                break;

            chassert(!file_metadata);

            const auto bucket = ObjectStorageQueueMetadata::getBucketForPath(
                object_info->getPath(),
                buckets_num,
                metadata->getBucketingMode(),
                metadata->getPartitioningMode(),
                metadata->getFilenameParser());
            auto bucket_it = keys_cache_per_bucket.find(bucket);
            if (bucket_it == keys_cache_per_bucket.end())
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Bucket {} not found in keys cache (buckets keys cache size: {}, expected buckets: {})",
                    bucket, keys_cache_per_bucket.size(), metadata->getBucketsNum());
            }
            BucketInfo & bucket_info = *bucket_it->second;

            LOG_TEST(log, "Found next file: {}, bucket: {}, current bucket: {}, cached_keys: {}",
                     object_info->getFileName(),
                     bucket,
                     current_bucket_holder ? toString(current_bucket_holder->getBucket()) : "None",
                     bucket_info.keys.size());

            if (current_bucket_holder)
            {
                if (current_bucket_holder->getBucket() != bucket)
                {
                    /// Acquired bucket differs from object's bucket,
                    /// put it into bucket's cache and continue.
                    bucket_info.keys.emplace_back(object_info, nullptr);
                    continue;
                }
                /// Bucket is already acquired, process the file.
                return {object_info, nullptr, current_bucket_holder->getBucketInfo()};
            }

            if (bucket_info.processor.has_value())
            {
                //LOG_TEST(
                //    log, "Will not process, should be processed by processor {}, current processor {}",
                //    bucket_info.processor.value(), processor);

                /// Bucket is already locked for processing by another thread.
                bucket_info.keys.emplace_back(object_info, nullptr);
                continue;
            }

            current_bucket_holder = tryAcquireBucket(bucket, bucket_info, *acquired_buckets, processor);
            if (current_bucket_holder)
            {
                if (!bucket_info.keys.empty())
                {
                    /// We have to maintain ordering between keys,
                    /// so if some keys are already in cache - start with them.
                    bucket_info.keys.emplace_back(object_info, nullptr);
                    std::tie(object_info, file_metadata) = bucket_info.keys.front();
                    bucket_info.keys.pop_front();
                }
                return {object_info, file_metadata, current_bucket_holder->getBucketInfo()};
            }

            //LOG_TEST(
            //    log, "Will not process, failed to acquire bucket {}, current processor {}",
            //    bucket, processor);

            /// Bucket is already locked for processing by another thread.
            bucket_info.keys.emplace_back(object_info, nullptr);
        }

        LOG_TEST(log, "Reached the end of file iterator");
        iterator_finished = true;

        if (keys_cache_per_bucket.end() == std::find_if(
                keys_cache_per_bucket.begin(),
                keys_cache_per_bucket.end(),
                [](const auto & v) { return !v.second->keys.empty(); }))
        {
            break;
        }
    }
    return {};
}

ObjectStorageQueueSource::ObjectStorageQueueSource(
    String name_,
    size_t processor_id_,
    std::shared_ptr<FileIterator> file_iterator_,
    StorageObjectStorageConfigurationPtr configuration_,
    ObjectStoragePtr object_storage_,
    ProcessingProgressPtr progress_,
    const ReadFromFormatInfo & read_from_format_info_,
    const std::optional<FormatSettings> & format_settings_,
    FormatParserSharedResourcesPtr parser_shared_resources_,
    const CommitSettings & commit_settings_,
    const AfterProcessingSettings & after_processing_settings_,
    std::shared_ptr<ObjectStorageQueueMetadata> files_metadata_,
    ContextPtr context_,
    size_t max_block_size_,
    const std::atomic<bool> & shutdown_called_,
    const std::atomic<bool> & table_is_being_dropped_,
    std::shared_ptr<ObjectStorageQueueLog> system_queue_log_,
    const StorageID & storage_id_,
    LoggerPtr log_,
    bool commit_once_processed_,
    bool add_deduplication_info_)
    : ISource(std::make_shared<const Block>(read_from_format_info_.source_header))
    , WithContext(context_)
    , name(std::move(name_))
    , processor_id(processor_id_)
    , file_iterator(file_iterator_)
    , configuration(configuration_)
    , object_storage(object_storage_)
    , progress(progress_)
    , read_from_format_info(read_from_format_info_)
    , format_settings(format_settings_)
    , parser_shared_resources(std::move(parser_shared_resources_))
    , commit_settings(commit_settings_)
    , after_processing_settings(after_processing_settings_)
    , files_metadata(files_metadata_)
    , max_block_size(max_block_size_)
    , mode(files_metadata->getTableMetadata().getMode())
    , shutdown_called(shutdown_called_)
    , table_is_being_dropped(table_is_being_dropped_)
    , system_queue_log(system_queue_log_)
    , storage_id(storage_id_)
    , commit_once_processed(commit_once_processed_)
    , add_deduplication_info(add_deduplication_info_)
    , insert_deduplication_version(context_->getServerSettings()[ServerSetting::insert_deduplication_version].value)
    , log(log_)
{
    if (commit_once_processed)
        transaction_start_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}

String ObjectStorageQueueSource::getName() const
{
    return name;
}

Chunk ObjectStorageQueueSource::generate()
{
    auto component_guard = Coordination::setCurrentComponent("ObjectStorageQueueSource::generate");
    Chunk chunk;
    try
    {
        chunk = generateImpl();
    }
    catch (...)
    {
        if (commit_once_processed)
            commit(false, getCurrentExceptionMessage(true));

        throw;
    }

    if (!chunk && commit_once_processed)
        commit(true);

    return chunk;
}

Chunk ObjectStorageQueueSource::generateImpl()
{
    while (true)
    {
        if (isCancelled())
        {
            if (reader)
                reader->cancel();

            /// Are there any started, but not finished files?
            if (processed_files.empty() || processed_files.back().state != FileState::Processing)
            {
                LOG_DEBUG(
                    log, "Reader was cancelled "
                    "(processed files: {}, last processed file state: {})",
                    processed_files.size(),
                    processed_files.empty() ? "None" : magic_enum::enum_name(processed_files.back().state));
                /// No unfinished files, just stop processing.
                break;
            }

            auto started_file = processed_files.back().metadata;
            /// Something must have been already read.
            chassert(started_file->getFileStatus()->processed_rows > 0);
            /// Mark file as Cancelled, such files will not be set as Failed.
            processed_files.back().state = FileState::Cancelled;
            /// Throw exception to avoid inserting half processed file to destination table.
            throw Exception(
                ErrorCodes::QUERY_WAS_CANCELLED,
                "Processing was cancelled (having unfinished file: {})", started_file->getPath());
        }

        if (shutdown_called)
        {
            LOG_TEST(log, "Shutdown was called"); /// test_drop_table depends on this log message

            /// Are there any started, but not finished files?
            if (processed_files.empty() || processed_files.back().state != FileState::Processing)
            {
                LOG_DEBUG(
                    log, "Shutdown was called "
                    "(processed files: {}, last processed file state: {})",
                    processed_files.size(),
                    processed_files.empty() ? "None" : magic_enum::enum_name(processed_files.back().state));
                /// No unfinished files, just stop processing.
                break;
            }

            auto started_file = processed_files.back().metadata;
            if (table_is_being_dropped)
            {
                /// Something must have been already read.
                chassert(started_file->getFileStatus()->processed_rows > 0);
                /// Mark file as Cancelled, such files will not be set as Failed.
                processed_files.back().state = FileState::Cancelled;
                /// Throw exception to avoid inserting half processed file to destination table.
                throw Exception(
                    ErrorCodes::QUERY_WAS_CANCELLED,
                    "Table is being dropped (having unfinished file: {})", started_file->getPath());
            }

            LOG_DEBUG(log, "Shutdown called, but file {} is partially processed ({} rows). "
                     "Will process the file fully and then shutdown",
                     started_file->getPath(), started_file->getFileStatus()->processed_rows.load());
        }

        FileMetadataPtr file_metadata;
        if (reader)
        {
            chassert(processed_files.back().state == FileState::Processing, toString(processed_files.back().state));
            chassert(
                processed_files.back().metadata->getPath() == reader.getObjectInfo()->getPath(),
                fmt::format("Mismatch {} vs {}", processed_files.back().metadata->getPath(),
                            reader.getObjectInfo()->getPath()));

            file_metadata = processed_files.back().metadata;
        }
        else
        {
            if (shutdown_called)
            {
                LOG_DEBUG(
                    log, "Shutdown was called "
                    "(processed files: {}, last processed file state: {})",
                    processed_files.size(),
                    processed_files.empty() ? "None" : magic_enum::enum_name(processed_files.back().state));
                /// Stop processing.
                break;
            }

            const auto context = getContext();
            reader = StorageObjectStorageSource::createReader(
                processor_id,
                file_iterator,
                configuration,
                object_storage,
                read_from_format_info,
                format_settings,
                context,
                nullptr,
                log,
                max_block_size,
                parser_shared_resources,
                nullptr,
                /* need_only_count */ false);

            if (!reader)
            {
                LOG_TEST(log, "No reader");
                break;
            }

            const auto * object_info = dynamic_cast<const ObjectStorageQueueObjectInfo *>(reader.getObjectInfo().get());
            file_metadata = object_info->file_metadata;

            if (commit_settings.max_processed_files_before_commit)
            {
                auto old_processed_files = progress->processed_files.fetch_add(1);
                if (old_processed_files >= commit_settings.max_processed_files_before_commit)
                {
                    LOG_DEBUG(log, "Number of max processed files before commit reached "
                            "(rows: {}, bytes: {}, files: {}, time: {})",
                            progress->processed_rows.load(), progress->processed_bytes.load(),
                            progress->processed_files.load(), progress->elapsed_time.elapsedSeconds());

                    --progress->processed_files;
                    file_iterator->returnForRetry(reader.getObjectInfo(), file_metadata);
                    break;
                }
            }

            LOG_TEST(log, "Will process file: {}", file_metadata->getPath());

            processed_files.emplace_back(file_metadata);
        }

        chassert(file_metadata);
        auto file_status = file_metadata->getFileStatus();
        const auto & path = file_metadata->getPath();

        LOG_TEST(log, "Processing file: {}", path);

        Chunk chunk;
        bool result = false;

        try
        {
            if (file_status->processed_rows > 0)
            {
                fiu_do_on(FailPoints::object_storage_queue_fail_in_the_middle_of_file, {
                    throw Exception(
                        ErrorCodes::UNKNOWN_EXCEPTION,
                        "Failed to read file. Processed rows: {}", file_status->processed_rows.load());
                });
            }

            auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::ObjectStorageQueuePullMicroseconds);

            result = reader->pull(chunk);
        }
        catch (...)
        {
            const auto message = getCurrentExceptionMessage(true);
            LOG_ERROR(
                log,
                "Got an error while pulling chunk: {}. Will set file {} as failed (processed rows: {})",
                message, path, file_status->processed_rows.load());

            processed_files.back().state = FileState::ErrorOnRead;
            processed_files.back().exception_during_read = message;

             if (file_status->processed_rows > 0)
             {
                 /// Fail the whole insert.
                 throw;
             }

            if (mode == ObjectStorageQueueMode::ORDERED)
            {
                /// Stop processing and commit what is already processed.
                /// because we must preserve order.
                return {};
            }
            else
            {
                /// Continue processing.
                /// This failed file will be committed along with processed files.
                reader = {};
                progress->processed_files -= 1;
                continue;
            }
        }

        if (result)
        {
            const auto & object_metadata = reader.getObjectInfo()->getObjectMetadata();
            const auto row_offset = file_status->processed_rows.load();

            std::string dedup_token;
            if (add_deduplication_info)
            {
                /// Etag is quoted for some reason.
                std::string etag = object_metadata->etag;
                if (etag.size() > 2 && etag.front() == '\"' && etag.back() == '\"')
                    etag = etag.substr(1, etag.size() - 2);

                /// Create unique token per chunk: etag + row offset
                dedup_token = fmt::format("{}:{}", etag, row_offset);

                auto deduplication_info = DeduplicationInfo::create(/*async_insert*/true, insert_deduplication_version);
                deduplication_info->setUserToken(dedup_token, chunk.getNumRows());
                chunk.getChunkInfos().add(std::move(deduplication_info));
            }

            LOG_TEST(log, "Read {} rows from file {} (file offset: {}, deduplication token for chunk: {})",
                     chunk.getNumRows(), path, row_offset, dedup_token);

            file_status->processed_rows += chunk.getNumRows();
            progress->processed_rows += chunk.getNumRows();
            progress->processed_bytes += chunk.bytes();

            ProfileEvents::increment(ProfileEvents::ObjectStorageQueueReadRows, chunk.getNumRows());
            ProfileEvents::increment(ProfileEvents::ObjectStorageQueueReadBytes, chunk.bytes());

            if (!read_from_format_info.hive_partition_columns_to_read_from_file_path.empty())
            {
                HivePartitioningUtils::addPartitionColumnsToChunk(
                    chunk,
                    read_from_format_info.hive_partition_columns_to_read_from_file_path,
                    path);
            }

            VirtualColumnUtils::addRequestedFileLikeStorageVirtualsToChunk(
                chunk,
                read_from_format_info.requested_virtual_columns,
                {
                    .path = path,
                    .size = object_metadata->size_bytes,
                    .last_modified = object_metadata->last_modified
                },
                getContext());

            return chunk;
        }

        ProfileEvents::increment(ProfileEvents::ObjectStorageQueueReadFiles);

        LOG_DEBUG(log,
                 "Processed file {}. Total processed files: {}, processed rows: {}, processed bytes: {}",
                 path, progress->processed_files.load(), progress->processed_rows.load(), progress->processed_bytes.load());

        processed_files.back().state = FileState::Processed;
        file_status->setProcessingEndTime();
        file_status = nullptr;
        reader = {};

        if (commit_settings.max_processed_files_before_commit
            && progress->processed_files >= commit_settings.max_processed_files_before_commit)
        {
            LOG_DEBUG(log, "Number of max processed files before commit reached "
                      "(rows: {}, bytes: {}, files: {}, time: {})",
                      progress->processed_rows.load(), progress->processed_bytes.load(), progress->processed_files.load(), progress->elapsed_time.elapsedSeconds());
            break;
        }

        if (commit_settings.max_processed_rows_before_commit
            && progress->processed_rows >= commit_settings.max_processed_rows_before_commit)
        {
            LOG_DEBUG(log, "Number of max processed rows before commit reached "
                      "(rows: {}, bytes: {}, files: {}, time: {})",
                      progress->processed_rows.load(), progress->processed_bytes.load(), progress->processed_files.load(), progress->elapsed_time.elapsedSeconds());
            break;
        }

        if (commit_settings.max_processed_bytes_before_commit
            && progress->processed_bytes >= commit_settings.max_processed_bytes_before_commit)
        {
            LOG_DEBUG(log, "Number of max processed bytes before commit reached "
                      "(rows: {}, bytes: {}, files: {}, time: {})",
                      progress->processed_rows.load(), progress->processed_bytes.load(), progress->processed_files.load(), progress->elapsed_time.elapsedSeconds());
            break;
        }

        if (commit_settings.max_processing_time_sec_before_commit
            && progress->elapsed_time.elapsedSeconds() >= static_cast<double>(commit_settings.max_processing_time_sec_before_commit))
        {
            LOG_DEBUG(log, "Max processing time before commit reached "
                      "(rows: {}, bytes: {}, files: {}, time: {})",
                      progress->processed_rows.load(), progress->processed_bytes.load(), progress->processed_files.load(), progress->elapsed_time.elapsedSeconds());
            break;
        }

    }

    return {};
}

void ObjectStorageQueueSource::prepareCommitRequests(
    Coordination::Requests & requests,
    bool insert_succeeded,
    StoredObjects & successful_files,
    PartitionLastProcessedFileInfoMap & file_map,
    LastProcessedFileInfoMapPtr created_nodes,
    const std::string & exception_message,
    int error_code)
{
    if (processed_files.empty())
        return;

    LOG_TEST(
        log,
        "Having {} files to set as {}",
        processed_files.size(),
        insert_succeeded ? "Processed" : "Failed");

    const bool is_ordered_mode = files_metadata->getTableMetadata().getMode() == ObjectStorageQueueMode::ORDERED;
    const bool use_buckets_for_processing = file_iterator->useBucketsForProcessing();
    const bool has_partitioning = files_metadata->getPartitioningMode() != ObjectStorageQueuePartitioningMode::NONE;
    std::map<size_t, size_t> last_processed_file_idx_per_bucket;

    /// For Ordered mode collect a map: bucket_id -> max_processed_path.
    /// If no buckets are used, we still do this for Ordered mode,
    /// just consider there will be only one bucket with id 0.
    if (insert_succeeded && is_ordered_mode)
    {
        for (size_t i = 0; i < processed_files.size(); ++i)
        {
            const auto & file_metadata = processed_files[i].metadata;
            const auto & file_path = file_metadata->getPath();
            const auto bucket = use_buckets_for_processing ? file_metadata->getBucket() : 0;

            if (processed_files[i].state != FileState::Processed)
                continue;

            auto [it, inserted] = last_processed_file_idx_per_bucket.emplace(bucket, i);
            if (!inserted
                && file_path > processed_files[it->second].metadata->getPath())
            {
                it->second = i;
            }
        }
    }

    /// We do not want to reduce retry count on certain errors,
    /// because their incidence does not depend on the user.
    const bool reduce_retry_count = !(error_code == ErrorCodes::TOO_MANY_PARTS
                                      || error_code == ErrorCodes::TABLE_IS_BEING_RESTARTED
                                      || error_code == ErrorCodes::TABLE_IS_READ_ONLY);

    for (size_t i = 0; i < processed_files.size(); ++i)
    {
        const auto & [file_state, file_metadata, exception_during_read] = processed_files[i];
        switch (file_state)
        {
            case FileState::Processed:
            {
                chassert(exception_during_read.empty());
                if (insert_succeeded)
                {
                    if (is_ordered_mode)
                    {
                        /// For Ordered mode we need to commit as Processed
                        /// only one max_processed_file per each bucket,
                        /// for all other files we only remove Processing nodes.
                        const auto bucket = use_buckets_for_processing ? file_metadata->getBucket() : 0;
                        if (last_processed_file_idx_per_bucket[bucket] == i)
                        {
                            file_metadata->prepareProcessedRequests(requests, created_nodes);
                        }
                        else
                        {
                            file_metadata->prepareResetProcessingRequests(requests);
                        }
                        if (has_partitioning)
                            file_metadata->preparePartitionProcessedMap(file_map);
                    }
                    else
                    {
                        file_metadata->prepareProcessedRequests(requests);
                    }
                    successful_files.push_back(StoredObject(file_metadata->getPath()));
                }
                else
                {
                    ProfileEvents::increment(ProfileEvents::ObjectStorageQueueExceptionsDuringInsert);

                    file_metadata->prepareFailedRequests(
                        requests,
                        exception_message,
                        reduce_retry_count);
                }
                break;
            }
            case FileState::Cancelled: [[fallthrough]];
            case FileState::Processing:
            {
                chassert(exception_during_read.empty());
                if (insert_succeeded)
                {
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Unexpected state {} of file {} while insert succeeded",
                        file_state, file_metadata->getPath());
                }

                if (file_state == FileState::Cancelled)
                    ProfileEvents::increment(ProfileEvents::ObjectStorageQueueCancelledFiles);
                else
                    ProfileEvents::increment(ProfileEvents::ObjectStorageQueueExceptionsDuringInsert);

                file_metadata->prepareFailedRequests(
                    requests,
                    exception_message,
                    reduce_retry_count);
                break;
            }
            case FileState::ErrorOnRead:
            {
                ProfileEvents::increment(ProfileEvents::ObjectStorageQueueExceptionsDuringRead);

                chassert(!exception_during_read.empty());
                file_metadata->prepareFailedRequests(
                    requests,
                    exception_during_read,
                    /* reduce_retry_count */true);
                break;
            }
        }
    }
}

void ObjectStorageQueueSource::preparePartitionProcessedRequests(
    Coordination::Requests & requests,
    const PartitionLastProcessedFileInfoMap & last_processed_file_per_partition)
{
    for (const auto & [partition_processed_path, last_processed_file_info] : last_processed_file_per_partition)
    {
        if (last_processed_file_info.exists)
        {
            requests.push_back(zkutil::makeSetRequest(
                partition_processed_path, last_processed_file_info.file_path, -1));
        }
        else
        {
            requests.push_back(zkutil::makeCreateRequest(
                partition_processed_path, last_processed_file_info.file_path, zkutil::CreateMode::Persistent));
        }
    }
}

void ObjectStorageQueueSource::finalizeCommit(
    bool insert_succeeded,
    UInt64 commit_id,
    time_t commit_time,
    time_t transaction_start_time_,
    const std::string & exception_message)
{
    if (processed_files.empty())
        return;

    std::exception_ptr finalize_exception;
    for (const auto & [file_state, file_metadata, exception_during_read] : processed_files)
    {
        try
        {
            switch (file_state)
            {
                case FileState::Processed:
                {
                    if (insert_succeeded)
                    {
                        file_metadata->finalizeProcessed();
                    }
                    else
                    {
                        file_metadata->finalizeFailed(exception_message);
                    }
                    break;
                }
                case FileState::Cancelled: [[fallthrough]];
                case FileState::Processing:
                {
                    if (insert_succeeded)
                    {
                        throw Exception(
                            ErrorCodes::LOGICAL_ERROR,
                            "Unexpected state {} of file {} while insert succeeded",
                            file_state, file_metadata->getPath());
                    }

                    file_metadata->finalizeFailed(exception_message);
                    break;
                }
                case FileState::ErrorOnRead:
                {
                    chassert(!exception_during_read.empty());
                    file_metadata->finalizeFailed(exception_during_read);
                    break;
                }
            }

            appendLogElement(
                file_metadata,
                /* processed */insert_succeeded && file_state == FileState::Processed,
                commit_id,
                commit_time,
                transaction_start_time_);
        }
        catch (...)
        {
            tryLogCurrentException(log);
            if (!finalize_exception)
                finalize_exception = std::current_exception();
        }
    }
    if (finalize_exception)
        std::rethrow_exception(finalize_exception);
}

void ObjectStorageQueueSource::commit(bool insert_succeeded, const std::string & exception_message)
{
    /// This method is only used for SELECT query, not for streaming to materialized views.
    /// Which is defined by passing a flag commit_once_processed.

    Coordination::Requests requests;
    StoredObjects successful_objects;
    PartitionLastProcessedFileInfoMap last_processed_file_per_partition;
    // `created_nodes` is nullptr here, because it is required only in mutithread case,
    // when `requests` is filled with several `prepareCommitRequests` calls.
    prepareCommitRequests(
        requests,
        insert_succeeded,
        successful_objects,
        last_processed_file_per_partition,
        /* created_nodes */ nullptr,
        exception_message);
    preparePartitionProcessedRequests(requests, last_processed_file_per_partition);

    if (!successful_objects.empty()
        && files_metadata->getTableMetadata().after_processing != ObjectStorageQueueAction::KEEP)
    {
        auto postProcessor = ObjectStorageQueuePostProcessor(
            getContext(),
            configuration->getType(),
            object_storage,
            getName(),
            files_metadata->getTableMetadata(),
            after_processing_settings);
        postProcessor.process(successful_objects);
    }

    auto zk_client = files_metadata->getZooKeeper();
    Coordination::Responses responses;

    auto zk_retry = ObjectStorageQueueMetadata::getKeeperRetriesControl(log);
    const auto & settings = getContext()->getSettingsRef();
    Coordination::Error code;
    size_t try_num = 0;
    zk_retry.retryLoop([&]
    {
        if (zk_retry.isRetry())
        {
            LOG_TRACE(
                log, "Failed to commit processed files at try {}/{}, will retry",
                try_num, toString(settings[Setting::keeper_max_retries].value));
        }
        ++try_num;
        code = zk_client->tryMulti(requests, responses);
    });

    if (code != Coordination::Error::ZOK)
        throw zkutil::KeeperMultiException(code, requests, responses);

    const auto commit_id = StorageObjectStorageQueue::generateCommitID();
    const auto commit_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

    finalizeCommit(insert_succeeded, commit_id, commit_time, transaction_start_time, exception_message);
    LOG_DEBUG(log, "Successfully committed {} requests", requests.size());
}

void ObjectStorageQueueSource::appendLogElement(
    const ObjectStorageQueueMetadata::FileMetadataPtr & file_metadata_,
    bool processed,
    UInt64 commit_id,
    time_t commit_time,
    time_t transaction_start_time_)
{
    if (!system_queue_log)
        return;

    const auto & file_path = file_metadata_->getPath();
    const auto & file_status = *file_metadata_->getFileStatus();

    ObjectStorageQueueLogElement elem{};
    {
        elem = ObjectStorageQueueLogElement
        {
            .event_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()),
            .database = storage_id.database_name,
            .table = storage_id.table_name,
            .uuid = toString(storage_id.uuid),
            .file_name = file_path,
            .rows_processed = file_status.processed_rows,
            .status = processed ? ObjectStorageQueueLogElement::ObjectStorageQueueStatus::Processed : ObjectStorageQueueLogElement::ObjectStorageQueueStatus::Failed,
            .processing_start_time = file_status.processing_start_time,
            .processing_end_time = file_status.processing_end_time,
            .exception = file_status.getException(),
            .commit_id = commit_id,
            .commit_time = commit_time,
            .transaction_start_time = transaction_start_time_,
            .get_object_time_ms = file_status.get_object_time_ms,
        };
    }
    system_queue_log->add(std::move(elem));
}

}
