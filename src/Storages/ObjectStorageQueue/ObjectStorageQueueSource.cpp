#include "config.h"

#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/logger_useful.h>
#include <Common/getRandomASCIIString.h>
#include <Storages/ObjectStorageQueue/ObjectStorageQueueSource.h>
#include <Storages/VirtualColumnUtils.h>
#include <Processors/Executors/PullingPipelineExecutor.h>


namespace ProfileEvents
{
    extern const Event ObjectStorageQueuePullMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

ObjectStorageQueueSource::ObjectStorageQueueObjectInfo::ObjectStorageQueueObjectInfo(
        const ObjectInfo & object_info,
        Metadata::FileMetadataPtr processing_holder_)
    : ObjectInfo(object_info.relative_path, object_info.metadata)
    , processing_holder(processing_holder_)
{
}

ObjectStorageQueueSource::FileIterator::FileIterator(
    std::shared_ptr<ObjectStorageQueueMetadata> metadata_,
    std::unique_ptr<GlobIterator> glob_iterator_,
    std::atomic<bool> & shutdown_called_,
    LoggerPtr logger_)
    : StorageObjectStorageSource::IIterator("ObjectStorageQueueIterator")
    , metadata(metadata_)
    , glob_iterator(std::move(glob_iterator_))
    , shutdown_called(shutdown_called_)
    , log(logger_)
{
}

size_t ObjectStorageQueueSource::FileIterator::estimatedKeysCount()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method estimateKeysCount is not implemented");
}

ObjectStorageQueueSource::ObjectInfoPtr ObjectStorageQueueSource::FileIterator::nextImpl(size_t processor)
{
    ObjectInfoPtr object_info;
    ObjectStorageQueueOrderedFileMetadata::BucketInfoPtr bucket_info;

    while (!shutdown_called)
    {
        if (metadata->useBucketsForProcessing())
            std::tie(object_info, bucket_info) = getNextKeyFromAcquiredBucket(processor);
        else
            object_info = glob_iterator->next(processor);

        if (!object_info)
            return {};

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

std::pair<ObjectStorageQueueSource::ObjectInfoPtr, ObjectStorageQueueOrderedFileMetadata::BucketInfoPtr>
ObjectStorageQueueSource::FileIterator::getNextKeyFromAcquiredBucket(size_t processor)
{
    /// We need this lock to maintain consistency between listing object storage directory
    /// and getting/putting result into listed_keys_cache.
    std::lock_guard lock(buckets_mutex);

    auto bucket_holder_it = bucket_holders.emplace(processor, nullptr).first;
    auto current_processor = toString(processor);

    LOG_TEST(
        log, "Current processor: {}, acquired bucket: {}",
        processor, bucket_holder_it->second ? toString(bucket_holder_it->second->getBucket()) : "None");

    while (true)
    {
        /// Each processing thread gets next path from glob_iterator->next()
        /// and checks if corresponding bucket is already acquired by someone.
        /// In case it is already acquired, they put the key into listed_keys_cache,
        /// so that the thread who acquired the bucket will be able to see
        /// those keys without the need to list object storage directory once again.
        if (bucket_holder_it->second)
        {
            const auto bucket = bucket_holder_it->second->getBucket();
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

                    return std::pair{object_info, bucket_holder_it->second->getBucketInfo()};
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
                /// Bucket is fully processed - release the bucket.
                bucket_holder_it->second->release();
                bucket_holder_it->second.reset();
            }
        }
        /// If processing thread has already acquired some bucket
        /// and while listing object storage directory gets a key which is in a different bucket,
        /// it puts the key into listed_keys_cache to allow others to process it,
        /// because one processing thread can acquire only one bucket at a time.
        /// Once a thread is finished with its acquired bucket, it checks listed_keys_cache
        /// to see if there are keys from buckets not acquired by anyone.
        if (!bucket_holder_it->second)
        {
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

                bucket_holder_it->second = metadata->tryAcquireBucket(bucket, current_processor);
                if (!bucket_holder_it->second)
                {
                    LOG_TEST(log, "Bucket {} is already locked for processing (keys: {})",
                             bucket, bucket_keys.size());
                    ++it;
                    continue;
                }

                bucket_processor = current_processor;

                /// Take the key from the front, the order is important.
                auto object_info = bucket_keys.front();
                bucket_keys.pop_front();

                LOG_TEST(log, "Acquired bucket: {}, will process file: {}",
                         bucket, object_info->getFileName());

                return std::pair{object_info, bucket_holder_it->second->getBucketInfo()};
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
                     bucket_holder_it->second ? toString(bucket_holder_it->second->getBucket()) : "None",
                     bucket_cache.keys.size());

            if (bucket_holder_it->second)
            {
                if (bucket_holder_it->second->getBucket() != bucket)
                {
                    /// Acquired bucket differs from object's bucket,
                    /// put it into bucket's cache and continue.
                    bucket_cache.keys.emplace_back(object_info);
                    continue;
                }
                /// Bucket is already acquired, process the file.
                return std::pair{object_info, bucket_holder_it->second->getBucketInfo()};
            }
            else
            {
                bucket_holder_it->second = metadata->tryAcquireBucket(bucket, current_processor);
                if (bucket_holder_it->second)
                {
                    bucket_cache.processor = current_processor;
                    if (!bucket_cache.keys.empty())
                    {
                        /// We have to maintain ordering between keys,
                        /// so if some keys are already in cache - start with them.
                        bucket_cache.keys.emplace_back(object_info);
                        object_info = bucket_cache.keys.front();
                        bucket_cache.keys.pop_front();
                    }
                    return std::pair{object_info, bucket_holder_it->second->getBucketInfo()};
                }
                else
                {
                    LOG_TEST(log, "Bucket {} is already locked for processing", bucket);
                    bucket_cache.keys.emplace_back(object_info);
                    continue;
                }
            }
        }
        else
        {
            if (bucket_holder_it->second)
            {
                bucket_holder_it->second->release();
                bucket_holder_it->second.reset();
            }

            LOG_TEST(log, "Reached the end of file iterator");
            iterator_finished = true;

            if (listed_keys_cache.empty())
                return {};
            else
                continue;
        }
    }
}

ObjectStorageQueueSource::ObjectStorageQueueSource(
    String name_,
    size_t processor_id_,
    const Block & header_,
    std::unique_ptr<StorageObjectStorageSource> internal_source_,
    std::shared_ptr<ObjectStorageQueueMetadata> files_metadata_,
    const ObjectStorageQueueAction & action_,
    RemoveFileFunc remove_file_func_,
    const NamesAndTypesList & requested_virtual_columns_,
    ContextPtr context_,
    const std::atomic<bool> & shutdown_called_,
    const std::atomic<bool> & table_is_being_dropped_,
    std::shared_ptr<ObjectStorageQueueLog> system_queue_log_,
    const StorageID & storage_id_,
    LoggerPtr log_)
    : ISource(header_)
    , WithContext(context_)
    , name(std::move(name_))
    , processor_id(processor_id_)
    , action(action_)
    , files_metadata(files_metadata_)
    , internal_source(std::move(internal_source_))
    , requested_virtual_columns(requested_virtual_columns_)
    , shutdown_called(shutdown_called_)
    , table_is_being_dropped(table_is_being_dropped_)
    , system_queue_log(system_queue_log_)
    , storage_id(storage_id_)
    , remove_file_func(remove_file_func_)
    , log(log_)
{
}

String ObjectStorageQueueSource::getName() const
{
    return name;
}

void ObjectStorageQueueSource::lazyInitialize(size_t processor)
{
    if (initialized)
        return;

    internal_source->lazyInitialize(processor);
    reader = std::move(internal_source->reader);
    if (reader)
        reader_future = std::move(internal_source->reader_future);
    initialized = true;
}

Chunk ObjectStorageQueueSource::generate()
{
    lazyInitialize(processor_id);

    while (true)
    {
        if (!reader)
            break;

        const auto * object_info = dynamic_cast<const ObjectStorageQueueObjectInfo *>(&reader.getObjectInfo());
        auto file_metadata = object_info->processing_holder;
        auto file_status = file_metadata->getFileStatus();

        if (isCancelled())
        {
            reader->cancel();

            if (processed_rows_from_file)
            {
                try
                {
                    file_metadata->setFailed("Cancelled");
                }
                catch (...)
                {
                    LOG_ERROR(log, "Failed to set file {} as failed: {}",
                             object_info->relative_path, getCurrentExceptionMessage(true));
                }

                appendLogElement(reader.getObjectInfo().getPath(), *file_status, processed_rows_from_file, false);
            }

            break;
        }

        const auto & path = reader.getObjectInfo().getPath();

        if (shutdown_called)
        {
            if (processed_rows_from_file == 0)
                break;

            if (table_is_being_dropped)
            {
                LOG_DEBUG(
                    log, "Table is being dropped, {} rows are already processed from {}, but file is not fully processed",
                    processed_rows_from_file, path);

                try
                {
                    file_metadata->setFailed("Table is dropped");
                }
                catch (...)
                {
                    LOG_ERROR(log, "Failed to set file {} as failed: {}",
                              object_info->relative_path, getCurrentExceptionMessage(true));
                }

                appendLogElement(path, *file_status, processed_rows_from_file, false);

                /// Leave the file half processed. Table is being dropped, so we do not care.
                break;
            }

            LOG_DEBUG(log, "Shutdown called, but file {} is partially processed ({} rows). "
                     "Will process the file fully and then shutdown",
                     path, processed_rows_from_file);
        }

        auto * prev_scope = CurrentThread::get().attachProfileCountersScope(&file_status->profile_counters);
        SCOPE_EXIT({ CurrentThread::get().attachProfileCountersScope(prev_scope); });
        /// FIXME:  if files are compressed, profile counters update does not work fully (object storage related counters are not saved). Why?

        try
        {
            auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::ObjectStorageQueuePullMicroseconds);

            Chunk chunk;
            if (reader->pull(chunk))
            {
                LOG_TEST(log, "Read {} rows from file: {}", chunk.getNumRows(), path);

                file_status->processed_rows += chunk.getNumRows();
                processed_rows_from_file += chunk.getNumRows();

                VirtualColumnUtils::addRequestedFileLikeStorageVirtualsToChunk(
                    chunk, requested_virtual_columns,
                    {
                        .path = path,
                        .size = reader.getObjectInfo().metadata->size_bytes
                    });


                return chunk;
            }
        }
        catch (...)
        {
            const auto message = getCurrentExceptionMessage(true);
            LOG_ERROR(log, "Got an error while pulling chunk. Will set file {} as failed. Error: {} ", path, message);

            file_metadata->setFailed(message);

            appendLogElement(path, *file_status, processed_rows_from_file, false);
            throw;
        }

        file_metadata->setProcessed();
        applyActionAfterProcessing(reader.getObjectInfo().relative_path);

        appendLogElement(path, *file_status, processed_rows_from_file, true);
        file_status.reset();
        processed_rows_from_file = 0;

        if (shutdown_called)
        {
            LOG_INFO(log, "Shutdown was called, stopping sync");
            break;
        }

        chassert(reader_future.valid());
        reader = reader_future.get();

        if (!reader)
            break;

        file_status = files_metadata->getFileStatus(reader.getObjectInfo().getPath());

        /// Even if task is finished the thread may be not freed in pool.
        /// So wait until it will be freed before scheduling a new task.
        internal_source->create_reader_pool->wait();
        reader_future = internal_source->createReaderAsync(processor_id);
    }

    return {};
}

void ObjectStorageQueueSource::applyActionAfterProcessing(const String & path)
{
    switch (action)
    {
        case ObjectStorageQueueAction::DELETE:
        {
            assert(remove_file_func);
            remove_file_func(path);
            break;
        }
        case ObjectStorageQueueAction::KEEP:
            break;
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
            .counters_snapshot = file_status_.profile_counters.getPartiallyAtomicSnapshot(),
            .processing_start_time = file_status_.processing_start_time,
            .processing_end_time = file_status_.processing_end_time,
            .exception = file_status_.getException(),
        };
    }
    system_queue_log->add(std::move(elem));
}

}
