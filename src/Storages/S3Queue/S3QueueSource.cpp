#include "config.h"

#if USE_AWS_S3
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/logger_useful.h>
#include <Common/getRandomASCIIString.h>
#include <Storages/S3Queue/S3QueueSource.h>
#include <Storages/VirtualColumnUtils.h>


namespace CurrentMetrics
{
    extern const Metric StorageS3Threads;
    extern const Metric StorageS3ThreadsActive;
}

namespace ProfileEvents
{
    extern const Event S3QueuePullMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int S3_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

StorageS3QueueSource::S3QueueKeyWithInfo::S3QueueKeyWithInfo(
        const std::string & key_,
        std::optional<S3::ObjectInfo> info_,
        Metadata::FileMetadataPtr processing_holder_)
    : StorageS3Source::KeyWithInfo(key_, info_)
    , processing_holder(processing_holder_)
{
}

StorageS3QueueSource::FileIterator::FileIterator(
    std::shared_ptr<S3QueueFilesMetadata> metadata_,
    std::unique_ptr<GlobIterator> glob_iterator_,
    std::atomic<bool> & shutdown_called_,
    LoggerPtr logger_)
    : metadata(metadata_)
    , glob_iterator(std::move(glob_iterator_))
    , current_processor(getRandomASCIIString(10)) /// TODO: add server uuid?
    , shutdown_called(shutdown_called_)
    , log(logger_)
{
}

StorageS3QueueSource::FileIterator::~FileIterator()
{
    releaseAndResetCurrentBucket();
}

void StorageS3QueueSource::FileIterator::releaseAndResetCurrentBucket()
{
    try
    {
        if (current_bucket.has_value())
        {
            metadata->releaseBucket(current_bucket.value());
            current_bucket.reset();
        }
    }
    catch (const zkutil::KeeperException & e)
    {
        if (e.code == Coordination::Error::ZSESSIONEXPIRED)
        {
            LOG_TRACE(log, "Session expired while releasing bucket");
        }
        if (e.code == Coordination::Error::ZNONODE)
        {
            LOG_TRACE(log, "Bucket {} does not exist. "
                      "This could happen because of an exprired session",
                      current_bucket.value());
        }
        else
        {
            LOG_ERROR(log, "Got unexpected exception while releasing bucket: {}",
                      getCurrentExceptionMessage(true));
            chassert(false);
        }
        current_bucket.reset();
    }
}

StorageS3QueueSource::KeyWithInfoPtr StorageS3QueueSource::FileIterator::getNextKeyFromAcquiredBucket()
{
    /// We need this lock to maintain consistency between listing s3 directory
    /// and getting/putting result into listed_keys_cache.
    std::lock_guard lock(buckets_mutex);

    while (true)
    {
        /// Each processing thread gets next path from glob_iterator->next()
        /// and checks if corresponding bucket is already acquired by someone.
        /// In case it is already acquired, they put the key into listed_keys_cache,
        /// so that the thread who acquired the bucket will be able to see
        /// those keys without the need to list s3 directory once again.
        if (current_bucket.has_value())
        {
            auto it = listed_keys_cache.find(current_bucket.value());
            if (it != listed_keys_cache.end())
            {
                /// `bucket_keys` -- keys we iterated so far and which were not taken for processing.
                /// `processor` -- processor id of the thread which has acquired the bucket.
                auto & [bucket_keys, processor] = it->second;

                /// Check correctness just in case.
                if (!processor.has_value() || processor.value() != current_processor)
                {
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Expected current processor {} to be equal to bucket's {} processor, "
                        "but have {}", current_processor, current_bucket.value(),
                        processor.has_value() ? processor.value() : Processor{});
                }

                /// Take next key to process
                if (!bucket_keys.empty())
                {
                    /// Take the key from the front, the order is important.
                    auto key_with_info = bucket_keys.front();
                    bucket_keys.pop_front();
                    return key_with_info;
                }

                /// No more keys in bucket, remove it from cache.
                listed_keys_cache.erase(it);
            }

            if (iterator_finished)
            {
                /// Bucket is fully processed - release the bucket.
                releaseAndResetCurrentBucket();
            }
        }
        /// If processing thread has already acquired some bucket
        /// and while listing s3 directory gets a key which is in a different bucket,
        /// it puts the key into listed_keys_cache to allow others to process it,
        /// because one processing thread can acquire only one bucket at a time.
        /// Once a thread is finished with its acquired bucket, it checks listed_keys_cache
        /// to see if there are keys from buckets not acquired by anyone.
        if (!current_bucket.has_value())
        {
            for (auto it = listed_keys_cache.begin(); it != listed_keys_cache.end();)
            {
                auto & [bucket, bucket_info] = *it;
                auto & [bucket_keys, processor] = bucket_info;

                if (processor.has_value())
                {
                    LOG_TEST(log, "Bucket {} is already locked for processing by {} (keys: {})",
                             bucket, processor.value(), bucket_keys.size());
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

                if (!metadata->tryAcquireBucket(bucket, current_processor))
                {
                    LOG_TEST(log, "Bucket {} is already locked for processing (keys: {})",
                             bucket, bucket_keys.size());
                    ++it;
                    continue;
                }

                current_bucket = bucket;
                processor = current_processor;

                /// Take the key from the front, the order is important.
                auto key_with_info = bucket_keys.front();
                bucket_keys.pop_front();
                return key_with_info;
            }
        }

        if (iterator_finished)
        {
            LOG_TEST(log, "Reached the end of file iterator and nothing left in keys cache");
            return {};
        }

        auto key_with_info = glob_iterator->next();
        if (key_with_info)
        {
            const auto bucket = metadata->getBucketForPath(key_with_info->key);

            LOG_TEST(log, "Found next file: {}, bucket: {}, current bucket: {}",
                     key_with_info->getFileName(), bucket,
                     current_bucket.has_value() ? toString(current_bucket.value()) : "None");

            if (current_bucket.has_value())
            {
                if (current_bucket.value() != bucket)
                {
                    listed_keys_cache[bucket].keys.emplace_back(key_with_info);
                    continue;
                }
                return key_with_info;
            }
            else
            {
                if (!metadata->tryAcquireBucket(bucket, current_processor))
                {
                    LOG_TEST(log, "Bucket {} is already locked for processing", bucket);
                    continue;
                }

                current_bucket = bucket;
                return key_with_info;
            }
        }
        else
        {
            releaseAndResetCurrentBucket();

            LOG_TEST(log, "Reached the end of file iterator");
            iterator_finished = true;

            if (listed_keys_cache.empty())
                return {};
            else
                continue;
        }
    }
}

StorageS3QueueSource::KeyWithInfoPtr StorageS3QueueSource::FileIterator::next()
{
    while (!shutdown_called)
    {
        auto val = metadata->useBucketsForProcessing() ? getNextKeyFromAcquiredBucket() : glob_iterator->next();
        if (!val)
            return {};

        if (shutdown_called)
        {
            LOG_TEST(log, "Shutdown was called, stopping file iterator");
            return {};
        }

        auto file_metadata = metadata->getFileMetadata(val->key);
        if (file_metadata->setProcessing())
        {
            return std::make_shared<S3QueueKeyWithInfo>(val->key, val->info, file_metadata);
        }
    }
    return {};
}

size_t StorageS3QueueSource::FileIterator::estimatedKeysCount()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method estimateKeysCount is not implemented");
}

StorageS3QueueSource::StorageS3QueueSource(
    String name_,
    const Block & header_,
    std::unique_ptr<StorageS3Source> internal_source_,
    std::shared_ptr<S3QueueFilesMetadata> files_metadata_,
    const S3QueueAction & action_,
    RemoveFileFunc remove_file_func_,
    const NamesAndTypesList & requested_virtual_columns_,
    ContextPtr context_,
    const std::atomic<bool> & shutdown_called_,
    const std::atomic<bool> & table_is_being_dropped_,
    std::shared_ptr<S3QueueLog> s3_queue_log_,
    const StorageID & storage_id_,
    LoggerPtr log_)
    : ISource(header_)
    , WithContext(context_)
    , name(std::move(name_))
    , action(action_)
    , files_metadata(files_metadata_)
    , internal_source(std::move(internal_source_))
    , requested_virtual_columns(requested_virtual_columns_)
    , shutdown_called(shutdown_called_)
    , table_is_being_dropped(table_is_being_dropped_)
    , s3_queue_log(s3_queue_log_)
    , storage_id(storage_id_)
    , remove_file_func(remove_file_func_)
    , log(log_)
{
}

StorageS3QueueSource::~StorageS3QueueSource()
{
    internal_source->create_reader_pool.wait();
}

String StorageS3QueueSource::getName() const
{
    return name;
}

void StorageS3QueueSource::lazyInitialize()
{
    if (initialized)
        return;

    internal_source->lazyInitialize();
    reader = std::move(internal_source->reader);
    if (reader)
        reader_future = std::move(internal_source->reader_future);
    initialized = true;
}

Chunk StorageS3QueueSource::generate()
{
    lazyInitialize();

    while (true)
    {
        if (!reader)
            break;

        const auto * key_with_info = dynamic_cast<const S3QueueKeyWithInfo *>(&reader.getKeyWithInfo());
        auto file_metadata = key_with_info->processing_holder;
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
                             key_with_info->key, getCurrentExceptionMessage(true));
                }

                appendLogElement(reader.getFile(), *file_status, processed_rows_from_file, false);
            }

            break;
        }

        if (shutdown_called)
        {
            if (processed_rows_from_file == 0)
                break;

            if (table_is_being_dropped)
            {
                LOG_DEBUG(
                    log, "Table is being dropped, {} rows are already processed from {}, but file is not fully processed",
                    processed_rows_from_file, reader.getFile());

                try
                {
                    file_metadata->setFailed("Table is dropped");
                }
                catch (...)
                {
                    LOG_ERROR(log, "Failed to set file {} as failed: {}",
                             key_with_info->key, getCurrentExceptionMessage(true));
                }

                appendLogElement(reader.getFile(), *file_status, processed_rows_from_file, false);

                /// Leave the file half processed. Table is being dropped, so we do not care.
                break;
            }

            LOG_DEBUG(log, "Shutdown called, but file {} is partially processed ({} rows). "
                     "Will process the file fully and then shutdown",
                     reader.getFile(), processed_rows_from_file);
        }

        auto * prev_scope = CurrentThread::get().attachProfileCountersScope(&file_status->profile_counters);
        SCOPE_EXIT({ CurrentThread::get().attachProfileCountersScope(prev_scope); });
        /// FIXME:  if files are compressed, profile counters update does not work fully (s3 related counters are not saved). Why?

        try
        {
            auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::S3QueuePullMicroseconds);

            Chunk chunk;
            if (reader->pull(chunk))
            {
                LOG_TEST(log, "Read {} rows from file: {}", chunk.getNumRows(), reader.getPath());

                file_status->processed_rows += chunk.getNumRows();
                processed_rows_from_file += chunk.getNumRows();

                VirtualColumnUtils::addRequestedPathFileAndSizeVirtualsToChunk(chunk, requested_virtual_columns, reader.getPath(), reader.getKeyWithInfo().info->size);
                return chunk;
            }
        }
        catch (...)
        {
            const auto message = getCurrentExceptionMessage(true);
            LOG_ERROR(log, "Got an error while pulling chunk. Will set file {} as failed. Error: {} ", reader.getFile(), message);

            file_metadata->setFailed(message);

            appendLogElement(reader.getFile(), *file_status, processed_rows_from_file, false);
            throw;
        }

        file_metadata->setProcessed();
        applyActionAfterProcessing(reader.getFile());

        appendLogElement(reader.getFile(), *file_status, processed_rows_from_file, true);
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

        file_status = files_metadata->getFileStatus(reader.getFile());

        /// Even if task is finished the thread may be not freed in pool.
        /// So wait until it will be freed before scheduling a new task.
        internal_source->create_reader_pool.wait();
        reader_future = internal_source->createReaderAsync();
    }

    return {};
}

void StorageS3QueueSource::applyActionAfterProcessing(const String & path)
{
    switch (action)
    {
        case S3QueueAction::DELETE:
        {
            assert(remove_file_func);
            remove_file_func(path);
            break;
        }
        case S3QueueAction::KEEP:
            break;
    }
}

void StorageS3QueueSource::appendLogElement(
    const std::string & filename,
    S3QueueFilesMetadata::FileStatus & file_status_,
    size_t processed_rows,
    bool processed)
{
    if (!s3_queue_log)
        return;

    S3QueueLogElement elem{};
    {
        std::lock_guard lock(file_status_.metadata_lock);
        elem = S3QueueLogElement
        {
            .event_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()),
            .database = storage_id.database_name,
            .table = storage_id.table_name,
            .uuid = toString(storage_id.uuid),
            .file_name = filename,
            .rows_processed = processed_rows,
            .status = processed ? S3QueueLogElement::S3QueueStatus::Processed : S3QueueLogElement::S3QueueStatus::Failed,
            .counters_snapshot = file_status_.profile_counters.getPartiallyAtomicSnapshot(),
            .processing_start_time = file_status_.processing_start_time,
            .processing_end_time = file_status_.processing_end_time,
            .exception = file_status_.last_exception,
        };
    }
    s3_queue_log->add(std::move(elem));
}

}

#endif
