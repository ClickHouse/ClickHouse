#include "config.h"

#if USE_AWS_S3
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/logger_useful.h>
#include <Storages/S3Queue/S3QueueSource.h>
#include <Storages/VirtualColumnUtils.h>


namespace CurrentMetrics
{
extern const Metric StorageS3Threads;
extern const Metric StorageS3ThreadsActive;
}

namespace ProfileEvents
{
extern const Event S3DeleteObjects;
extern const Event S3ListObjects;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int S3_ERROR;
}

StorageS3QueueSource::FileIterator::FileIterator(
    std::shared_ptr<S3QueueFilesMetadata> metadata_, std::unique_ptr<GlobIterator> glob_iterator_)
    : metadata(metadata_) , glob_iterator(std::move(glob_iterator_))
{
}

StorageS3QueueSource::KeyWithInfo StorageS3QueueSource::FileIterator::next()
{
    /// List results in s3 are always returned in UTF-8 binary order.
    /// (https://docs.aws.amazon.com/AmazonS3/latest/userguide/ListingKeysUsingAPIs.html)

    while (true)
    {
        KeyWithInfo val = glob_iterator->next();
        if (val.key.empty())
            return {};
        if (metadata->trySetFileAsProcessing(val.key))
            return val;
    }
}

StorageS3QueueSource::StorageS3QueueSource(
    String name_,
    const Block & header_,
    std::unique_ptr<StorageS3Source> internal_source_,
    std::shared_ptr<S3QueueFilesMetadata> files_metadata_,
    const S3QueueAction & action_,
    RemoveFileFunc remove_file_func_,
    const NamesAndTypesList & requested_virtual_columns_,
    ContextPtr context_)
    : ISource(header_)
    , WithContext(context_)
    , name(std::move(name_))
    , action(action_)
    , files_metadata(files_metadata_)
    , internal_source(std::move(internal_source_))
    , requested_virtual_columns(requested_virtual_columns_)
    , remove_file_func(remove_file_func_)
    , log(&Poco::Logger::get("StorageS3QueueSource"))
{
    reader = std::move(internal_source->reader);
    if (reader)
        reader_future = std::move(internal_source->reader_future);
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
        if (isCancelled() || !reader)
        {
            if (reader)
                reader->cancel();
            break;
        }

        try
        {
            Chunk chunk;
            if (reader->pull(chunk))
            {
                LOG_TEST(log, "Read {} rows from file: {}", chunk.getNumRows(), reader.getPath());
                VirtualColumnUtils::addRequestedPathAndFileVirtualsToChunk(chunk, requested_virtual_columns, reader.getPath());
                return chunk;
            }
        }
        catch (const Exception & e)
        {
            LOG_ERROR(log, "Exception in chunk pulling: {} ", e.displayText());
            files_metadata->setFileFailed(reader.getFile(), e.message());
            throw;
        }

        files_metadata->setFileProcessed(reader.getFile());
        applyActionAfterProcessing(reader.getFile());

        chassert(reader_future.valid());
        reader = reader_future.get();

        if (!reader)
            break;

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

}

#endif
