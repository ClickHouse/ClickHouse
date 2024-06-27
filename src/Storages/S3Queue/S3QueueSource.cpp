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
        Metadata::ProcessingNodeHolderPtr processing_holder_)
    : StorageS3Source::KeyWithInfo(key_, info_)
    , processing_holder(processing_holder_)
{
}

StorageS3QueueSource::FileIterator::FileIterator(
    std::shared_ptr<S3QueueFilesMetadata> metadata_,
    std::unique_ptr<GlobIterator> glob_iterator_,
    size_t current_shard_,
    std::atomic<bool> & shutdown_called_)
    : metadata(metadata_)
    , glob_iterator(std::move(glob_iterator_))
    , shutdown_called(shutdown_called_)
    , log(&Poco::Logger::get("StorageS3QueueSource"))
    , sharded_processing(metadata->isShardedProcessing())
    , current_shard(current_shard_)
{
    if (sharded_processing)
    {
        for (const auto & id : metadata->getProcessingIdsForShard(current_shard))
            sharded_keys.emplace(id, std::deque<KeyWithInfoPtr>{});
    }
}

StorageS3QueueSource::KeyWithInfoPtr StorageS3QueueSource::FileIterator::next(size_t idx)
{
    while (!shutdown_called)
    {
        KeyWithInfoPtr val{nullptr};

        {
            std::unique_lock lk(sharded_keys_mutex, std::defer_lock);
            if (sharded_processing)
            {
                /// To make sure order on keys in each shard in sharded_keys
                /// we need to check sharded_keys and to next() under lock.
                lk.lock();

                if (auto it = sharded_keys.find(idx); it != sharded_keys.end())
                {
                    auto & keys = it->second;
                    if (!keys.empty())
                    {
                        val = keys.front();
                        keys.pop_front();
                        chassert(idx == metadata->getProcessingIdForPath(val->key));
                    }
                }
                else
                {
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                                    "Processing id {} does not exist (Expected ids: {})",
                                    idx, fmt::join(metadata->getProcessingIdsForShard(current_shard), ", "));
                }
            }

            if (!val)
            {
                val = glob_iterator->next();
                if (val && sharded_processing)
                {
                    const auto processing_id_for_key = metadata->getProcessingIdForPath(val->key);
                    if (idx != processing_id_for_key)
                    {
                        if (metadata->isProcessingIdBelongsToShard(processing_id_for_key, current_shard))
                        {
                            LOG_TEST(log, "Putting key {} into queue of processor {} (total: {})",
                                     val->key, processing_id_for_key, sharded_keys.size());

                            if (auto it = sharded_keys.find(processing_id_for_key); it != sharded_keys.end())
                            {
                                it->second.push_back(val);
                            }
                            else
                            {
                                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                                "Processing id {} does not exist (Expected ids: {})",
                                                processing_id_for_key, fmt::join(metadata->getProcessingIdsForShard(current_shard), ", "));
                            }
                        }
                        continue;
                    }
                }
            }
        }

        if (!val)
            return {};

        if (shutdown_called)
        {
            LOG_TEST(log, "Shutdown was called, stopping file iterator");
            return {};
        }

        auto processing_holder = metadata->trySetFileAsProcessing(val->key);
        if (shutdown_called)
        {
            LOG_TEST(log, "Shutdown was called, stopping file iterator");
            return {};
        }

        LOG_TEST(log, "Checking if can process key {} for processing_id {}", val->key, idx);

        if (processing_holder)
        {
            return std::make_shared<S3QueueKeyWithInfo>(val->key, val->info, processing_holder);
        }
        else if (sharded_processing
                 && metadata->getFileStatus(val->key)->state == S3QueueFilesMetadata::FileStatus::State::Processing)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "File {} is processing by someone else in sharded processing. "
                            "It is a bug", val->key);
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
    size_t processing_id_,
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
    , processing_id(processing_id_)
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

    internal_source->lazyInitialize(processing_id);
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
        auto file_status = key_with_info->processing_holder->getFileStatus();

        if (isCancelled())
        {
            reader->cancel();

            if (processed_rows_from_file)
            {
                try
                {
                    files_metadata->setFileFailed(key_with_info->processing_holder, "Cancelled");
                }
                catch (...)
                {
                    tryLogCurrentException(__PRETTY_FUNCTION__);
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
                    files_metadata->setFileFailed(key_with_info->processing_holder, "Table is dropped");
                }
                catch (...)
                {
                    tryLogCurrentException(__PRETTY_FUNCTION__);
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

            files_metadata->setFileFailed(key_with_info->processing_holder, message);

            appendLogElement(reader.getFile(), *file_status, processed_rows_from_file, false);
            throw;
        }

        files_metadata->setFileProcessed(key_with_info->processing_holder);
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
        reader_future = internal_source->createReaderAsync(processing_id);
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
