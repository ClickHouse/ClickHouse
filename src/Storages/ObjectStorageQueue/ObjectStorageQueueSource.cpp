#include "config.h"

#include <Common/ProfileEvents.h>
#include <Common/FailPoint.h>
#include <Common/CurrentMetrics.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/logger_useful.h>
#include <Common/getRandomASCIIString.h>
#include <Common/parseGlobs.h>
#include <Core/Settings.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/Context.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueSource.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueUnorderedFileMetadata.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueOrderedFileMetadata.h>
#include <Storages/VirtualColumnUtils.h>
#include <Disks/ObjectStorages/ObjectStorageIterator.h>
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
        const ObjectInfo & object_info,
        ObjectStorageQueueMetadata::FileMetadataPtr file_metadata_)
    : ObjectInfo(object_info.relative_path, object_info.metadata)
    , file_metadata(file_metadata_)
{
}

ObjectStorageQueueSource::FileIterator::FileIterator(
    std::shared_ptr<ObjectStorageQueueMetadata> metadata_,
    ObjectStoragePtr object_storage_,
    ConfigurationPtr configuration_,
    const StorageID & storage_id_,
    size_t list_objects_batch_size_,
    const ActionsDAG::Node * predicate_,
    const NamesAndTypesList & virtual_columns_,
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
    , file_deletion_on_processed_enabled(file_deletion_on_processed_enabled_)
    , mode(metadata->getTableMetadata().getMode())
    , enable_hash_ring_filtering(enable_hash_ring_filtering_)
    , storage_id(storage_id_)
    , shutdown_called(shutdown_called_)
    , log(logger_)
{
    if (configuration->isNamespaceWithGlobs())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expression can not have wildcards inside namespace name");

    if (!configuration->isPathWithGlobs())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Using glob iterator with path without globs is not allowed (used path: {})",
            configuration->getPath());
    }

    const auto globbed_key = configuration_->getPath();
    object_storage_iterator = object_storage->iterate(configuration->getPathWithoutGlobs(), list_objects_batch_size_);

    matcher = std::make_unique<re2::RE2>(makeRegexpPatternFromGlobs(globbed_key));
    if (!matcher->ok())
    {
        throw Exception(
            ErrorCodes::CANNOT_COMPILE_REGEXP,
            "Cannot compile regex from glob ({}): {}",
            globbed_key, matcher->error());
    }

    recursive = globbed_key == "/**";
    if (auto filter_dag = VirtualColumnUtils::createPathAndFileFilterDAG(predicate_, virtual_columns))
    {
        VirtualColumnUtils::buildSetsForDAG(*filter_dag, context_);
        filter_expr = std::make_shared<ExpressionActions>(std::move(*filter_dag));
    }
}

bool ObjectStorageQueueSource::FileIterator::isFinished()
{
    std::lock_guard lock(mutex);
    LOG_TEST(log, "Iterator finished: {}, objects to retry: {}", iterator_finished.load(), objects_to_retry.size());
    return iterator_finished
        && std::all_of(listed_keys_cache.begin(), listed_keys_cache.end(), [](const auto & v) { return v.second.keys.empty(); })
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

    if (current_batch_processed)
    {
        file_metadatas.clear();
        Source::ObjectInfos new_batch;
        while (new_batch.empty())
        {
            auto result = object_storage_iterator->getCurrentBatchAndScheduleNext();
            if (!result.has_value())
            {
                is_finished = true;
                return {};
            }

            LOG_TEST(log, "Received batch of size: {}", result->size());

            new_batch = std::move(result.value());
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

                VirtualColumnUtils::filterByPathOrFile(
                    new_batch, paths, filter_expr, virtual_columns, getContext());

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
                for (size_t i = 0; i < new_batch.size(); ++i)
                {
                    file_metadatas[i] = metadata->getFileMetadata(
                        new_batch[i]->relative_path,
                        /* bucket_info */{}); /// No buckets for Unordered mode.

                    auto set_processing_result = file_metadatas[i]->prepareSetProcessingRequests(requests);
                    if (set_processing_result.has_value())
                    {
                        result_indexes[i] = set_processing_result.value();
                        ++num_successful_objects;
                    }
                    else
                    {
                        new_batch[i] = nullptr;
                        file_metadatas[i] = nullptr;
                    }
                }

                Coordination::Responses responses;
                auto zk_client = Context::getGlobalContextInstance()->getZooKeeper();
                auto code = zk_client->tryMulti(requests, responses);
                if (code == Coordination::Error::ZOK)
                {
                    ProfileEvents::increment(ProfileEvents::ObjectStorageQueueTrySetProcessingSucceeded, num_successful_objects);

                    LOG_TEST(log, "Successfully set {} files as processing", new_batch.size());

                    for (size_t i = 0; i < new_batch.size(); ++i)
                    {
                        if (!new_batch[i])
                            continue;

                        const auto & response_indexes = result_indexes[i];
                        const auto & set_response = dynamic_cast<const Coordination::SetResponse &>(
                            *responses[response_indexes.set_processing_id_node_idx].get());

                        file_metadatas[i]->finalizeProcessing(set_response.stat.version);
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

void ObjectStorageQueueSource::FileIterator::filterProcessableFiles(Source::ObjectInfos & objects)
{
    std::vector<std::string> paths;
    paths.reserve(objects.size());
    for (const auto & object : objects)
        paths.push_back(object->getPath());

    if (enable_hash_ring_filtering && mode == ObjectStorageQueueMode::UNORDERED)
        metadata->filterOutForProcessor(paths, storage_id);

    if (mode == ObjectStorageQueueMode::UNORDERED)
        ObjectStorageQueueUnorderedFileMetadata::filterOutProcessedAndFailed(paths, metadata->getPath(), log);
    else
        ObjectStorageQueueOrderedFileMetadata::filterOutProcessedAndFailed(paths, metadata->getPath(), metadata->getBucketsNum(), log);

    std::unordered_set<std::string> paths_set;
    std::ranges::move(paths, std::inserter(paths_set, paths_set.end()));

    Source::ObjectInfos result;
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

        if (metadata->useBucketsForProcessing())
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
            LOG_TEST(log, "Shutdown was called, stopping file iterator");
            return {};
        }

        if (!file_metadata)
        {
            file_metadata = metadata->getFileMetadata(object_info->relative_path, bucket_info);
            if (!file_metadata->trySetProcessing())
                continue;
        }

        if (file_deletion_on_processed_enabled
            && !object_storage->exists(StoredObject(object_info->relative_path)))
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
    if (metadata->useBucketsForProcessing())
    {
        const auto bucket = metadata->getBucketForPath(object_info->relative_path);
        std::lock_guard lock(mutex);
        listed_keys_cache[bucket].keys.emplace_front(object_info, file_metadata);
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
    for (const auto & [processor, holders] : bucket_holders)
    {
        LOG_TEST(log, "Releasing {} bucket holders for processor {}", holders.size(), processor);

        for (auto it = holders.begin(); it != holders.end(); ++it)
        {
            const auto & holder = *it;
            const auto bucket = holder->getBucketInfo()->bucket;
            if (!holder->isFinished())
            {
                /// Only the last holder in the list of holders can be non-finished.
                chassert(std::next(it) == holders.end());

                /// Do not release non-finished bucket holder. We will continue processing it.
                LOG_TEST(log, "Bucket {} is not finished yet, will not release it", bucket);
                break;
            }

            /// Release bucket lock.
            holder->release();

            /// Reset bucket processor in cached state.
            auto cached_info = listed_keys_cache.find(bucket);
            if (cached_info != listed_keys_cache.end())
                cached_info->second.processor.reset();
        }
    }
}

ObjectStorageQueueSource::FileIterator::NextKeyFromBucket
ObjectStorageQueueSource::FileIterator::getNextKeyFromAcquiredBucket(size_t processor)
{
    auto bucket_holder_it = bucket_holders.emplace(processor, std::vector<BucketHolderPtr>{}).first;
    BucketHolder * current_bucket_holder = bucket_holder_it->second.empty() || bucket_holder_it->second.back()->isFinished()
        ? nullptr
        : bucket_holder_it->second.back().get();

    auto current_processor = toString(processor);

    LOG_TEST(
        log, "Current processor: {}, acquired bucket: {}",
        processor, current_bucket_holder ? toString(current_bucket_holder->getBucket()) : "None");

    while (true)
    {
        /// Each processing thread gets next path
        /// and checks if corresponding bucket is already acquired by someone.
        /// In case it is already acquired, they put the key into listed_keys_cache,
        /// so that the thread who acquired the bucket will be able to see
        /// those keys without the need to list s3 directory once again.
        if (current_bucket_holder)
        {
            const auto bucket = current_bucket_holder->getBucket();
            auto it = listed_keys_cache.find(bucket);
            if (it != listed_keys_cache.end())
            {
                /// `bucket_keys` -- keys we iterated so far and which were not taken for processing.
                /// `bucket_processor` -- processor id of the thread which has acquired the bucket.
                auto & [bucket_keys, bucket_processor] = it->second;

                /// Check correctness just in case.
                if (!bucket_processor.has_value())
                {
                    bucket_processor = current_processor;
                }
                else if (bucket_processor.value() != current_processor)
                {
                    if (current_bucket_holder->isZooKeeperSessionExpired())
                    {
                        LOG_TRACE(log, "ZooKeeper session expired, bucket no longer held");
                        current_bucket_holder = {};
                    }
                    else
                    {
                        throw Exception(
                            ErrorCodes::LOGICAL_ERROR,
                            "Expected current processor {} to be equal to {} for bucket {}",
                            current_processor,
                            bucket_processor.has_value() ? toString(bucket_processor.value()) : "None",
                            bucket);
                    }
                }

                if (current_bucket_holder)
                {
                    if (!bucket_keys.empty())
                    {
                        /// Take the key from the front, the order is important.
                        auto [object_info, file_metadata] = bucket_keys.front();
                        bucket_keys.pop_front();

                            LOG_TEST(log, "Current bucket: {}, will process file: {}",
                                    bucket, object_info->getFileName());

                        return {object_info, file_metadata, current_bucket_holder->getBucketInfo()};
                    }

                    LOG_TEST(log, "Cache of bucket {} is empty", bucket);

                    /// No more keys in bucket, remove it from cache.
                    listed_keys_cache.erase(it);
                }
            }
            else
            {
                LOG_TEST(log, "Cache of bucket {} is empty", bucket);
            }

            if (current_bucket_holder && iterator_finished)
            {
                /// Bucket is fully processed, but we will release it later
                /// - once we write and commit files via commit() method.
                current_bucket_holder->setFinished();
            }
        }

        if (current_bucket_holder && current_bucket_holder->isZooKeeperSessionExpired())
        {
            LOG_TRACE(log, "ZooKeeper session expired, bucket {} not longer hold", current_bucket_holder->getBucket());
            current_bucket_holder = {};
        }

        /// If processing thread has already acquired some bucket
        /// and while listing object storage directory gets a key which is in a different bucket,
        /// it puts the key into listed_keys_cache to allow others to process it,
        /// because one processing thread can acquire only one bucket at a time.
        /// Once a thread is finished with its acquired bucket, it checks listed_keys_cache
        /// to see if there are keys from buckets not acquired by anyone.
        if (!current_bucket_holder)
        {
            LOG_TEST(log, "Checking caches keys: {}", listed_keys_cache.size());

            for (auto it = listed_keys_cache.begin(); it != listed_keys_cache.end();)
            {
                auto & [bucket, bucket_info] = *it;
                auto & [bucket_keys, bucket_processor] = bucket_info;

                LOG_TEST(log, "Bucket: {}, cached keys: {}, processor: {}",
                         bucket, bucket_keys.size(), bucket_processor.has_value() ? toString(bucket_processor.value()) : "None");

                if (bucket_processor.has_value())
                {
                    LOG_TEST(log, "Bucket {} is already locked for processing by {} (keys: {})",
                             bucket, bucket_processor.value(), bucket_keys.size());
                    ++it;
                    continue;
                }

                if (bucket_keys.empty())
                {
                    /// No more keys in bucket, remove it from cache.
                    /// We still might add new keys to this bucket if !iterator_finished.
                    it = listed_keys_cache.erase(it);
                    continue;
                }

                auto acquired_bucket = metadata->tryAcquireBucket(bucket, current_processor);
                if (!acquired_bucket)
                {
                    LOG_TEST(log, "Bucket {} is already locked for processing (keys: {})",
                             bucket, bucket_keys.size());
                    ++it;
                    continue;
                }

                bucket_holder_it->second.push_back(acquired_bucket);
                current_bucket_holder = bucket_holder_it->second.back().get();

                bucket_processor = current_processor;

                /// Take the key from the front, the order is important.
                auto [object_info, file_metadata] = bucket_keys.front();
                bucket_keys.pop_front();

                LOG_TEST(log, "Acquired bucket: {}, will process file: {}",
                         bucket, object_info->getFileName());

                return {object_info, file_metadata, current_bucket_holder->getBucketInfo()};
            }
        }

        if (iterator_finished)
        {
            LOG_TEST(log, "Reached the end of file iterator and nothing left in keys cache");
            return {};
        }

        auto [object_info, file_metadata] = next();
        chassert(!file_metadata);
        if (object_info)
        {
            const auto bucket = metadata->getBucketForPath(object_info->relative_path);
            auto & bucket_cache = listed_keys_cache[bucket];

            LOG_TEST(log, "Found next file: {}, bucket: {}, current bucket: {}, cached_keys: {}",
                     object_info->getFileName(), bucket,
                     current_bucket_holder ? toString(current_bucket_holder->getBucket()) : "None",
                     bucket_cache.keys.size());

            if (current_bucket_holder)
            {
                if (current_bucket_holder->getBucket() != bucket)
                {
                    /// Acquired bucket differs from object's bucket,
                    /// put it into bucket's cache and continue.
                    bucket_cache.keys.emplace_back(object_info, nullptr);
                    continue;
                }
                /// Bucket is already acquired, process the file.
                return {object_info, nullptr, current_bucket_holder->getBucketInfo()};
            }

            auto acquired_bucket = metadata->tryAcquireBucket(bucket, current_processor);
            if (acquired_bucket)
            {
                bucket_holder_it->second.push_back(acquired_bucket);
                current_bucket_holder = bucket_holder_it->second.back().get();

                bucket_cache.processor = current_processor;
                if (!bucket_cache.keys.empty())
                {
                    /// We have to maintain ordering between keys,
                    /// so if some keys are already in cache - start with them.
                    bucket_cache.keys.emplace_back(object_info, nullptr);
                    std::tie(object_info, file_metadata) = bucket_cache.keys.front();
                    bucket_cache.keys.pop_front();
                }
                return {object_info, file_metadata, current_bucket_holder->getBucketInfo()};
            }

            LOG_TEST(log, "Bucket {} is already locked for processing", bucket);
            bucket_cache.keys.emplace_back(object_info, nullptr);
            continue;
        }

        LOG_TEST(log, "Reached the end of file iterator");
        iterator_finished = true;

        if (listed_keys_cache.empty())
            return {};
    }
}

ObjectStorageQueueSource::ObjectStorageQueueSource(
    String name_,
    size_t processor_id_,
    std::shared_ptr<FileIterator> file_iterator_,
    ConfigurationPtr configuration_,
    ObjectStoragePtr object_storage_,
    ProcessingProgressPtr progress_,
    const ReadFromFormatInfo & read_from_format_info_,
    const std::optional<FormatSettings> & format_settings_,
    FormatParserGroupPtr parser_group_,
    const CommitSettings & commit_settings_,
    std::shared_ptr<ObjectStorageQueueMetadata> files_metadata_,
    ContextPtr context_,
    size_t max_block_size_,
    const std::atomic<bool> & shutdown_called_,
    const std::atomic<bool> & table_is_being_dropped_,
    std::shared_ptr<ObjectStorageQueueLog> system_queue_log_,
    const StorageID & storage_id_,
    LoggerPtr log_,
    bool commit_once_processed_)
    : ISource(read_from_format_info_.source_header)
    , WithContext(context_)
    , name(std::move(name_))
    , processor_id(processor_id_)
    , file_iterator(file_iterator_)
    , configuration(configuration_)
    , object_storage(object_storage_)
    , progress(progress_)
    , read_from_format_info(read_from_format_info_)
    , format_settings(format_settings_)
    , parser_group(std::move(parser_group_))
    , commit_settings(commit_settings_)
    , files_metadata(files_metadata_)
    , max_block_size(max_block_size_)
    , mode(files_metadata->getTableMetadata().getMode())
    , shutdown_called(shutdown_called_)
    , table_is_being_dropped(table_is_being_dropped_)
    , system_queue_log(system_queue_log_)
    , storage_id(storage_id_)
    , commit_once_processed(commit_once_processed_)
    , log(log_)
{
}

String ObjectStorageQueueSource::getName() const
{
    return name;
}

Chunk ObjectStorageQueueSource::generate()
{
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
            LOG_TEST(log, "Shutdown was called");

            /// Are there any started, but not finished files?
            if (processed_files.empty() || processed_files.back().state != FileState::Processing)
            {
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
                LOG_TEST(log, "Shutdown called");
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
                parser_group,
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
                    LOG_TRACE(log, "Number of max processed files before commit reached "
                            "(rows: {}, bytes: {}, files: {}, time: {})",
                            progress->processed_rows.load(), progress->processed_bytes.load(),
                            progress->processed_files.load(), progress->elapsed_time.elapsedSeconds());

                    --progress->processed_files;
                    file_iterator->returnForRetry(reader.getObjectInfo(), file_metadata);
                    break;
                }
            }

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
            LOG_TEST(log, "Read {} rows from file: {}", chunk.getNumRows(), path);

            file_status->processed_rows += chunk.getNumRows();
            progress->processed_rows += chunk.getNumRows();
            progress->processed_bytes += chunk.bytes();

            ProfileEvents::increment(ProfileEvents::ObjectStorageQueueReadRows, chunk.getNumRows());
            ProfileEvents::increment(ProfileEvents::ObjectStorageQueueReadBytes, chunk.bytes());

            const auto & object_metadata = reader.getObjectInfo()->metadata;

            VirtualColumnUtils::addRequestedFileLikeStorageVirtualsToChunk(
                chunk, read_from_format_info.requested_virtual_columns,
                {
                    .path = path,
                    .size = object_metadata->size_bytes,
                    .last_modified = object_metadata->last_modified
                }, getContext());

            return chunk;
        }

        ProfileEvents::increment(ProfileEvents::ObjectStorageQueueReadFiles);

        LOG_TEST(log,
                 "Processed file {}. Total processed files: {}, processed rows: {}, processed bytes: {}",
                 path, progress->processed_files.load(), progress->processed_rows.load(), progress->processed_bytes.load());

        processed_files.back().state = FileState::Processed;
        file_status->setProcessingEndTime();
        file_status.reset();
        reader = {};

        if (commit_settings.max_processed_files_before_commit
            && progress->processed_files >= commit_settings.max_processed_files_before_commit)
        {
            LOG_TRACE(log, "Number of max processed files before commit reached "
                      "(rows: {}, bytes: {}, files: {}, time: {})",
                      progress->processed_rows.load(), progress->processed_bytes.load(), progress->processed_files.load(), progress->elapsed_time.elapsedSeconds());
            break;
        }

        if (commit_settings.max_processed_rows_before_commit
            && progress->processed_rows >= commit_settings.max_processed_rows_before_commit)
        {
            LOG_TRACE(log, "Number of max processed rows before commit reached "
                      "(rows: {}, bytes: {}, files: {}, time: {})",
                      progress->processed_rows.load(), progress->processed_bytes.load(), progress->processed_files.load(), progress->elapsed_time.elapsedSeconds());
            break;
        }

        if (commit_settings.max_processed_bytes_before_commit
            && progress->processed_bytes >= commit_settings.max_processed_bytes_before_commit)
        {
            LOG_TRACE(log, "Number of max processed bytes before commit reached "
                      "(rows: {}, bytes: {}, files: {}, time: {})",
                      progress->processed_rows.load(), progress->processed_bytes.load(), progress->processed_files.load(), progress->elapsed_time.elapsedSeconds());
            break;
        }

        if (commit_settings.max_processing_time_sec_before_commit
            && progress->elapsed_time.elapsedSeconds() >= commit_settings.max_processing_time_sec_before_commit)
        {
            LOG_TRACE(log, "Max processing time before commit reached "
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
    const bool use_buckets_for_processing = files_metadata->useBucketsForProcessing();
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
                            file_metadata->prepareProcessedRequests(requests);
                        }
                        else
                        {
                            file_metadata->prepareResetProcessingRequests(requests);
                        }
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

void ObjectStorageQueueSource::finalizeCommit(bool insert_succeeded, const std::string & exception_message)
{
    if (processed_files.empty())
        return;

    for (const auto & [file_state, file_metadata, exception_during_read] : processed_files)
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
            /* processed */insert_succeeded && file_state == FileState::Processed);
    }
}

void ObjectStorageQueueSource::commit(bool insert_succeeded, const std::string & exception_message)
{
    /// This method is only used for SELECT query, not for streaming to materialized views.
    /// Which is defined by passing a flag commit_once_processed.

    Coordination::Requests requests;
    StoredObjects successful_objects;
    prepareCommitRequests(requests, insert_succeeded, successful_objects, exception_message);

    if (!successful_objects.empty()
        && files_metadata->getTableMetadata().after_processing == ObjectStorageQueueAction::DELETE)
    {
        /// We do need to apply after-processing action before committing requests to keeper.
        /// See explanation in ObjectStorageQueueSource::FileIterator::nextImpl().
        object_storage->removeObjectsIfExist(successful_objects);
    }

    auto zk_client = getContext()->getZooKeeper();
    Coordination::Responses responses;
    auto code = zk_client->tryMulti(requests, responses);
    if (code != Coordination::Error::ZOK)
        throw zkutil::KeeperMultiException(code, requests, responses);

    finalizeCommit(insert_succeeded, exception_message);
    LOG_TRACE(log, "Successfully committed {} requests", requests.size());
}

void ObjectStorageQueueSource::appendLogElement(
    const ObjectStorageQueueMetadata::FileMetadataPtr & file_metadata_,
    bool processed)
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
        };
    }
    system_queue_log->add(std::move(elem));
}

}
