#include "config.h"

#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/logger_useful.h>
#include <Common/getRandomASCIIString.h>
#include <Core/Settings.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueSource.h>
#include <Storages/VirtualColumnUtils.h>
#include <Processors/Executors/PullingPipelineExecutor.h>


namespace ProfileEvents
{
    extern const Event ObjectStorageQueuePullMicroseconds;
}

namespace DB
{
namespace Setting
{
    extern const SettingsMaxThreads max_parsing_threads;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

ObjectStorageQueueSource::ObjectStorageQueueObjectInfo::ObjectStorageQueueObjectInfo(
        const Source::ObjectInfo & object_info,
        ObjectStorageQueueMetadata::FileMetadataPtr file_metadata_)
    : Source::ObjectInfo(object_info.relative_path, object_info.metadata)
    , file_metadata(file_metadata_)
{
}

ObjectStorageQueueSource::FileIterator::FileIterator(
    std::shared_ptr<ObjectStorageQueueMetadata> metadata_,
    std::unique_ptr<Source::GlobIterator> glob_iterator_,
    std::atomic<bool> & shutdown_called_,
    LoggerPtr logger_)
    : StorageObjectStorageSource::IIterator("ObjectStorageQueueIterator")
    , metadata(metadata_)
    , glob_iterator(std::move(glob_iterator_))
    , shutdown_called(shutdown_called_)
    , log(logger_)
{
}

bool ObjectStorageQueueSource::FileIterator::isFinished() const
{
     LOG_TEST(log, "Iterator finished: {}, objects to retry: {}", iterator_finished, objects_to_retry.size());
     return iterator_finished
         && std::all_of(listed_keys_cache.begin(), listed_keys_cache.end(), [](const auto & v) { return v.second.keys.empty(); })
         && objects_to_retry.empty();
}

size_t ObjectStorageQueueSource::FileIterator::estimatedKeysCount()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method estimateKeysCount is not implemented");
}

ObjectStorageQueueSource::Source::ObjectInfoPtr ObjectStorageQueueSource::FileIterator::nextImpl(size_t processor)
{
    Source::ObjectInfoPtr object_info;
    ObjectStorageQueueOrderedFileMetadata::BucketInfoPtr bucket_info;

    while (!shutdown_called)
    {
        if (metadata->useBucketsForProcessing())
        {
            std::lock_guard lock(mutex);
            std::tie(object_info, bucket_info) = getNextKeyFromAcquiredBucket(processor);
        }
        else
        {
            std::lock_guard lock(mutex);
            if (objects_to_retry.empty())
            {
                object_info = glob_iterator->next(processor);
                if (!object_info)
                    iterator_finished = true;
            }
            else
            {
                object_info = objects_to_retry.front();
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

        auto file_metadata = metadata->getFileMetadata(object_info->relative_path, bucket_info);
        if (file_metadata->setProcessing())
            return std::make_shared<ObjectStorageQueueObjectInfo>(*object_info, file_metadata);
    }
    return {};
}

void ObjectStorageQueueSource::FileIterator::returnForRetry(Source::ObjectInfoPtr object_info)
{
    chassert(object_info);
    if (metadata->useBucketsForProcessing())
    {
        const auto bucket = metadata->getBucketForPath(object_info->relative_path);
        std::lock_guard lock(mutex);
        listed_keys_cache[bucket].keys.emplace_front(object_info);
    }
    else
    {
        std::lock_guard lock(mutex);
        objects_to_retry.push_back(object_info);
    }
}

void ObjectStorageQueueSource::FileIterator::releaseFinishedBuckets()
{
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

std::pair<ObjectStorageQueueSource::Source::ObjectInfoPtr, ObjectStorageQueueOrderedFileMetadata::BucketInfoPtr>
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
        /// Each processing thread gets next path from glob_iterator->next()
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
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Expected current processor {} to be equal to {} for bucket {}",
                        current_processor,
                        bucket_processor.has_value() ? toString(bucket_processor.value()) : "None",
                        bucket);
                }

                /// Take next key to process
                if (!bucket_keys.empty())
                {
                    /// Take the key from the front, the order is important.
                    auto object_info = bucket_keys.front();
                    bucket_keys.pop_front();

                    LOG_TEST(log, "Current bucket: {}, will process file: {}",
                             bucket, object_info->getFileName());

                    return std::pair{object_info, current_bucket_holder->getBucketInfo()};
                }

                LOG_TEST(log, "Cache of bucket {} is empty", bucket);

                /// No more keys in bucket, remove it from cache.
                listed_keys_cache.erase(it);
            }
            else
            {
                LOG_TEST(log, "Cache of bucket {} is empty", bucket);
            }

            if (iterator_finished)
            {
                /// Bucket is fully processed, but we will release it later
                /// - once we write and commit files via commit() method.
                current_bucket_holder->setFinished();
            }
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
                auto object_info = bucket_keys.front();
                bucket_keys.pop_front();

                LOG_TEST(log, "Acquired bucket: {}, will process file: {}",
                         bucket, object_info->getFileName());

                return std::pair{object_info, current_bucket_holder->getBucketInfo()};
            }
        }

        if (iterator_finished)
        {
            LOG_TEST(log, "Reached the end of file iterator and nothing left in keys cache");
            return {};
        }

        auto object_info = glob_iterator->next(processor);
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
                    bucket_cache.keys.emplace_back(object_info);
                    continue;
                }
                /// Bucket is already acquired, process the file.
                return std::pair{object_info, current_bucket_holder->getBucketInfo()};
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
                    bucket_cache.keys.emplace_back(object_info);
                    object_info = bucket_cache.keys.front();
                    bucket_cache.keys.pop_front();
                }
                return std::pair{object_info, current_bucket_holder->getBucketInfo()};
            }

            LOG_TEST(log, "Bucket {} is already locked for processing", bucket);
            bucket_cache.keys.emplace_back(object_info);
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
    const ReadFromFormatInfo & read_from_format_info_,
    const std::optional<FormatSettings> & format_settings_,
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
    , read_from_format_info(read_from_format_info_)
    , format_settings(format_settings_)
    , commit_settings(commit_settings_)
    , files_metadata(files_metadata_)
    , max_block_size(max_block_size_)
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
    {
        commit(true);
    }
    return chunk;
}

Chunk ObjectStorageQueueSource::generateImpl()
{
    while (true)
    {
        if (!reader)
        {
            if (shutdown_called)
            {
                LOG_TEST(log, "Shutdown called");
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
                nullptr,
                context,
                nullptr,
                log,
                max_block_size,
                context->getSettingsRef()[Setting::max_parsing_threads].value,
                /* need_only_count */ false);

            if (!reader)
            {
                LOG_TEST(log, "No reader");
                break;
            }
        }

        const auto * object_info = dynamic_cast<const ObjectStorageQueueObjectInfo *>(reader.getObjectInfo().get());
        auto file_metadata = object_info->file_metadata;
        auto file_status = file_metadata->getFileStatus();
        const auto & path = reader.getObjectInfo()->getPath();

        if (isCancelled())
        {
            reader->cancel();

            if (processed_rows_from_file)
            {
                try
                {
                    file_metadata->setFailed("Cancelled", /* reduce_retry_count */true, /* overwrite_status */false);
                }
                catch (...)
                {
                    LOG_ERROR(log, "Failed to set file {} as failed: {}",
                             object_info->relative_path, getCurrentExceptionMessage(true));
                }
            }

            LOG_TEST(log, "Query is cancelled");
            break;
        }

        if (shutdown_called)
        {
            LOG_TEST(log, "Shutdown called");

            if (processed_rows_from_file == 0)
                break;

            if (table_is_being_dropped)
            {
                LOG_DEBUG(
                    log, "Table is being dropped, {} rows are already processed from {}, but file is not fully processed",
                    processed_rows_from_file, path);

                try
                {
                    file_metadata->setFailed("Table is dropped", /* reduce_retry_count */true, /* overwrite_status */false);
                }
                catch (...)
                {
                    LOG_ERROR(log, "Failed to set file {} as failed: {}",
                              object_info->relative_path, getCurrentExceptionMessage(true));
                }

                /// Leave the file half processed. Table is being dropped, so we do not care.
                break;
            }

            LOG_DEBUG(log, "Shutdown called, but file {} is partially processed ({} rows). "
                     "Will process the file fully and then shutdown",
                     path, processed_rows_from_file);
        }

        try
        {
            auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::ObjectStorageQueuePullMicroseconds);

            Chunk chunk;
            if (reader->pull(chunk))
            {
                LOG_TEST(log, "Read {} rows from file: {}", chunk.getNumRows(), path);

                file_status->processed_rows += chunk.getNumRows();
                processed_rows_from_file += chunk.getNumRows();
                total_processed_rows += chunk.getNumRows();
                total_processed_bytes += chunk.bytes();

                VirtualColumnUtils::addRequestedFileLikeStorageVirtualsToChunk(
                    chunk, read_from_format_info.requested_virtual_columns,
                    {
                        .path = path,
                        .size = reader.getObjectInfo()->metadata->size_bytes
                    }, getContext());

                return chunk;
            }
        }
        catch (...)
        {
            const auto message = getCurrentExceptionMessage(true);
            LOG_ERROR(log, "Got an error while pulling chunk. Will set file {} as failed. Error: {} ", path, message);

            failed_during_read_files.push_back(file_metadata);
            file_status->onFailed(getCurrentExceptionMessage(true));

            if (processed_rows_from_file == 0)
            {
                if (file_status->retries < file_metadata->getMaxTries())
                    file_iterator->returnForRetry(reader.getObjectInfo());

                /// If we did not process any rows from the failed file,
                /// commit all previously processed files,
                /// not to lose the work already done.
                return {};
            }

            throw;
        }

        file_status->setProcessingEndTime();
        file_status.reset();
        reader = {};

        processed_rows_from_file = 0;
        processed_files.push_back(file_metadata);

        if (commit_settings.max_processed_files_before_commit
            && processed_files.size() == commit_settings.max_processed_files_before_commit)
        {
            LOG_TRACE(log, "Number of max processed files before commit reached "
                      "(rows: {}, bytes: {}, files: {})",
                      total_processed_rows, total_processed_bytes, processed_files.size());
            break;
        }

        if (commit_settings.max_processed_rows_before_commit
            && total_processed_rows == commit_settings.max_processed_rows_before_commit)
        {
            LOG_TRACE(log, "Number of max processed rows before commit reached "
                      "(rows: {}, bytes: {}, files: {})",
                      total_processed_rows, total_processed_bytes, processed_files.size());
            break;
        }
        if (commit_settings.max_processed_bytes_before_commit && total_processed_bytes == commit_settings.max_processed_bytes_before_commit)
        {
            LOG_TRACE(
                log,
                "Number of max processed bytes before commit reached "
                "(rows: {}, bytes: {}, files: {})",
                total_processed_rows,
                total_processed_bytes,
                processed_files.size());
            break;
        }
        if (commit_settings.max_processing_time_sec_before_commit
            && total_stopwatch.elapsedSeconds() >= commit_settings.max_processing_time_sec_before_commit)
        {
            LOG_TRACE(
                log,
                "Max processing time before commit reached "
                "(rows: {}, bytes: {}, files: {})",
                total_processed_rows,
                total_processed_bytes,
                processed_files.size());
            break;
        }
    }

    return {};
}

void ObjectStorageQueueSource::commit(bool success, const std::string & exception_message)
{
    LOG_TEST(log, "Having {} files to set as {}, failed files: {}",
             processed_files.size(), success ? "Processed" : "Failed", failed_during_read_files.size());

    for (const auto & file_metadata : processed_files)
    {
        if (success)
        {
            file_metadata->setProcessed();
            applyActionAfterProcessing(file_metadata->getPath());
        }
        else
        {
            file_metadata->setFailed(
                exception_message,
                /* reduce_retry_count */false,
                /* overwrite_status */true);

        }
        appendLogElement(file_metadata->getPath(), *file_metadata->getFileStatus(), processed_rows_from_file, /* processed */success);
    }

    for (const auto & file_metadata : failed_during_read_files)
    {
        /// `exception` from commit args is from insertion to storage.
        /// Here we do not used it as failed_during_read_files were not inserted into storage, but skipped.
        file_metadata->setFailed(
            file_metadata->getFileStatus()->getException(),
            /* reduce_retry_count */true,
            /* overwrite_status */false);

        appendLogElement(file_metadata->getPath(), *file_metadata->getFileStatus(), processed_rows_from_file, /* processed */false);
    }
}

void ObjectStorageQueueSource::applyActionAfterProcessing(const String & path)
{
    if (files_metadata->getTableMetadata().after_processing == "delete")
    {
        object_storage->removeObject(StoredObject(path));
    }
}

void ObjectStorageQueueSource::appendLogElement(
    const std::string & filename,
    ObjectStorageQueueMetadata::FileStatus & file_status_,
    size_t processed_rows,
    bool processed)
{
    if (!system_queue_log)
        return;

    ObjectStorageQueueLogElement elem{};
    {
        elem = ObjectStorageQueueLogElement
        {
            .event_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()),
            .database = storage_id.database_name,
            .table = storage_id.table_name,
            .uuid = toString(storage_id.uuid),
            .file_name = filename,
            .rows_processed = processed_rows,
            .status = processed ? ObjectStorageQueueLogElement::ObjectStorageQueueStatus::Processed : ObjectStorageQueueLogElement::ObjectStorageQueueStatus::Failed,
            .processing_start_time = file_status_.processing_start_time,
            .processing_end_time = file_status_.processing_end_time,
            .exception = file_status_.getException(),
        };
    }
    system_queue_log->add(std::move(elem));
}

}
