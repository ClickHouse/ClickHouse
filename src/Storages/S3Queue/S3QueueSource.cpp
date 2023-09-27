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
}

StorageS3QueueSource::S3QueueKeyWithInfo::S3QueueKeyWithInfo(
        const std::string & key_,
        std::optional<S3::ObjectInfo> info_,
        std::unique_ptr<Metadata::ProcessingHolder> processing_holder_,
        std::shared_ptr<Metadata::FileStatus> file_status_)
    : StorageS3Source::KeyWithInfo(key_, info_)
    , processing_holder(std::move(processing_holder_))
    , file_status(file_status_)
{
}

StorageS3QueueSource::FileIterator::FileIterator(
    std::shared_ptr<S3QueueFilesMetadata> metadata_, std::unique_ptr<GlobIterator> glob_iterator_)
    : metadata(metadata_) , glob_iterator(std::move(glob_iterator_))
{
}

StorageS3QueueSource::KeyWithInfoPtr StorageS3QueueSource::FileIterator::next()
{
    /// List results in s3 are always returned in UTF-8 binary order.
    /// (https://docs.aws.amazon.com/AmazonS3/latest/userguide/ListingKeysUsingAPIs.html)

    while (true)
    {
        KeyWithInfoPtr val = glob_iterator->next();

        if (!val)
            return {};

        if (auto processing_holder = metadata->trySetFileAsProcessing(val->key); processing_holder)
        {
            return std::make_shared<S3QueueKeyWithInfo>(val->key, val->info, std::move(processing_holder), nullptr);
        }
    }
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
    std::shared_ptr<S3QueueLog> s3_queue_log_,
    const StorageID & storage_id_)
    : ISource(header_)
    , WithContext(context_)
    , name(std::move(name_))
    , action(action_)
    , files_metadata(files_metadata_)
    , internal_source(std::move(internal_source_))
    , requested_virtual_columns(requested_virtual_columns_)
    , shutdown_called(shutdown_called_)
    , s3_queue_log(s3_queue_log_)
    , storage_id(storage_id_)
    , s3_queue_user_id(fmt::format("{}:{}", CurrentThread::getQueryId(), getRandomASCIIString(8)))
    , remove_file_func(remove_file_func_)
    , log(&Poco::Logger::get("StorageS3QueueSource"))
{
    reader = std::move(internal_source->reader);
    if (reader)
    {
        reader_future = std::move(internal_source->reader_future);
        file_status = files_metadata->getFileStatus(reader.getFile());
    }
}

StorageS3QueueSource::~StorageS3QueueSource()
{
    internal_source->create_reader_pool.wait();
}

String StorageS3QueueSource::getName() const
{
    return name;
}

Chunk StorageS3QueueSource::generate()
{
    while (true)
    {
        if (!reader)
            break;

        if (isCancelled())
        {
            reader->cancel();
            break;
        }

        auto * prev_scope = CurrentThread::get().attachProfileCountersScope(&file_status->profile_counters);
        SCOPE_EXIT({ CurrentThread::get().attachProfileCountersScope(prev_scope); });

        try
        {
            auto timer = DB::CurrentThread::getProfileEvents().timer(ProfileEvents::S3QueuePullMicroseconds);

            Chunk chunk;
            if (reader->pull(chunk))
            {
                LOG_TEST(log, "Read {} rows from file: {}", chunk.getNumRows(), reader.getPath());

                file_status->processed_rows += chunk.getNumRows();
                // file_status->profile_counters.increment(ProfileEvents::S3QueuePullMicroseconds, timer.get());
                processed_rows_from_file += chunk.getNumRows();

                VirtualColumnUtils::addRequestedPathAndFileVirtualsToChunk(chunk, requested_virtual_columns, reader.getPath());
                return chunk;
            }
        }
        catch (const Exception & e)
        {
            LOG_ERROR(log, "Exception in chunk pulling: {} ", e.displayText());
            files_metadata->setFileFailed(reader.getFile(), e.message());
            appendLogElement(reader.getFile(), *file_status, processed_rows_from_file, false);
            throw;
        }

        files_metadata->setFileProcessed(reader.getFile());
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

void StorageS3QueueSource::appendLogElement(const std::string & filename, const S3QueueFilesMetadata::FileStatus & file_status_, size_t processed_rows, bool processed)
{
    if (!s3_queue_log)
        return;

    S3QueueLogElement elem
    {
        .event_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()),
        .file_name = filename,
        .rows_processed = processed_rows,
        .status = processed ? S3QueueLogElement::S3QueueStatus::Processed : S3QueueLogElement::S3QueueStatus::Failed,
        .counters_snapshot = file_status_.profile_counters.getPartiallyAtomicSnapshot(),
        .processing_start_time = file_status_.processing_start_time,
        .processing_end_time = file_status_.processing_end_time,
    };
    s3_queue_log->add(std::move(elem));
}

}

#endif
