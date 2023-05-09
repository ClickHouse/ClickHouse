#include <algorithm>
#include <Common/ProfileEvents.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include "IO/ParallelReadBuffer.h"
#include "Parsers/ASTCreateQuery.h"
#include "config.h"

#if USE_AWS_S3

#    include <Common/isValidUTF8.h>

#    include <Functions/FunctionsConversion.h>

#    include <IO/S3/Requests.h>
#    include <IO/S3Common.h>

#    include <Interpreters/TreeRewriter.h>

#    include <Parsers/ASTFunction.h>
#    include <Parsers/ASTInsertQuery.h>

#    include <Storages/NamedCollectionsHelpers.h>
#    include <Storages/PartitionedSink.h>
#    include <Storages/ReadFromStorageProgress.h>
#    include <Storages/S3Queue/S3QueueSource.h>
#    include <Storages/StorageS3.h>
#    include <Storages/StorageS3Settings.h>
#    include <Storages/VirtualColumnUtils.h>
#    include <Storages/getVirtualsForStorage.h>

#    include <Disks/IO/AsynchronousReadIndirectBufferFromRemoteFS.h>
#    include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#    include <Disks/ObjectStorages/StoredObject.h>

#    include <IO/ReadBufferFromS3.h>

#    include <Formats/FormatFactory.h>

#    include <Processors/Formats/IInputFormat.h>
#    include <Processors/Formats/IOutputFormat.h>
#    include <Processors/Transforms/AddingDefaultsTransform.h>

#    include <QueryPipeline/QueryPipelineBuilder.h>

#    include <DataTypes/DataTypeString.h>

#    include <Common/CurrentMetrics.h>
#    include <Common/NamedCollections/NamedCollections.h>
#    include <Common/parseGlobs.h>

#    include <Processors/ISource.h>
#    include <Processors/Sinks/SinkToStorage.h>

namespace fs = std::filesystem;


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


StorageS3QueueSource::QueueGlobIterator::QueueGlobIterator(
    const S3::Client & client_,
    const S3::URI & globbed_uri_,
    ASTPtr query,
    const Block & virtual_header,
    ContextPtr context,
    UInt64 & max_poll_size_,
    StorageS3QueueSource::KeysWithInfo * read_keys_,
    const S3Settings::RequestSettings & request_settings_)
    : max_poll_size(max_poll_size_)
    , bucket(globbed_uri_.bucket)
    , glob_iterator(std::make_unique<StorageS3QueueSource::DisclosedGlobIterator>(
          client_, globbed_uri_, query, virtual_header, context, read_keys_, request_settings_))
{
    while (true)
    {
        KeyWithInfo val = glob_iterator->next();
        if (val.key.empty())
        {
            break;
        }
        keys_buf.push_back(val);
    }
}

Strings StorageS3QueueSource::QueueGlobIterator::filterProcessingFiles(
    const S3QueueMode & engine_mode, std::unordered_set<String> & exclude_keys, const String & max_file)
{
    for (KeyWithInfo val : keys_buf)
    {
        auto full_path = bucket + '/' + val.key;
        if (exclude_keys.find(full_path) != exclude_keys.end())
        {
            LOG_INFO(log, "Found in exclude keys {}", val.key);
            continue;
        }
        if ((engine_mode == S3QueueMode::ORDERED) && (full_path.compare(max_file) <= 0))
        {
            continue;
        }
        if ((processing_keys.size() < max_poll_size) || (engine_mode == S3QueueMode::ORDERED))
        {
            processing_keys.push_back(val);
        }
        else
        {
            break;
        }
    }

    if (engine_mode == S3QueueMode::ORDERED)
    {
        std::sort(
            processing_keys.begin(),
            processing_keys.end(),
            [](const KeyWithInfo & lhs, const KeyWithInfo & rhs) { return lhs.key.compare(rhs.key) < 0; });
        if (processing_keys.size() > max_poll_size)
        {
            processing_keys.erase(processing_keys.begin() + max_poll_size, processing_keys.end());
        }
    }

    Strings keys;
    for (auto v : processing_keys)
    {
        keys.push_back(bucket + '/' + v.key);
    }
    processing_keys.push_back(KeyWithInfo());

    processing_iterator = processing_keys.begin();
    return keys;
}


StorageS3QueueSource::KeyWithInfo StorageS3QueueSource::QueueGlobIterator::next()
{
    std::lock_guard lock(mutex);
    if (processing_iterator != processing_keys.end())
    {
        return *processing_iterator++;
    }

    return KeyWithInfo();
}

size_t StorageS3QueueSource::QueueGlobIterator::getTotalSize() const
{
    return glob_iterator->getTotalSize();
}


Block StorageS3QueueSource::getHeader(Block sample_block, const std::vector<NameAndTypePair> & requested_virtual_columns)
{
    for (const auto & virtual_column : requested_virtual_columns)
        sample_block.insert({virtual_column.type->createColumn(), virtual_column.type, virtual_column.name});

    return sample_block;
}

StorageS3QueueSource::StorageS3QueueSource(
    const std::vector<NameAndTypePair> & requested_virtual_columns_,
    const String & format_,
    String name_,
    const Block & sample_block_,
    ContextPtr context_,
    std::optional<FormatSettings> format_settings_,
    const ColumnsDescription & columns_,
    UInt64 max_block_size_,
    const S3Settings::RequestSettings & request_settings_,
    String compression_hint_,
    const std::shared_ptr<const S3::Client> & client_,
    const String & bucket_,
    const String & version_id_,
    std::shared_ptr<IIterator> file_iterator_,
    std::shared_ptr<S3QueueHolder> queue_holder_,
    const S3QueueAction & action_,
    const size_t download_thread_num_)
    : ISource(getHeader(sample_block_, requested_virtual_columns_))
    , WithContext(context_)
    , name(std::move(name_))
    , bucket(bucket_)
    , version_id(version_id_)
    , format(format_)
    , columns_desc(columns_)
    , request_settings(request_settings_)
    , client(client_)
    , queue_holder(queue_holder_)
    , requested_virtual_columns(requested_virtual_columns_)
    , file_iterator(file_iterator_)
    , action(action_)
{
    internal_source = std::make_shared<StorageS3Source>(
        requested_virtual_columns_,
        format_,
        name_,
        sample_block_,
        context_,
        format_settings_,
        columns_,
        max_block_size_,
        request_settings_,
        compression_hint_,
        client_,
        bucket_,
        version_id_,
        file_iterator,
        download_thread_num_);
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

        Chunk chunk;
        try
        {
            if (reader->pull(chunk))
            {
                UInt64 num_rows = chunk.getNumRows();

                const auto & file_path = reader.getPath();
                size_t total_size = file_iterator->getTotalSize();
                if (num_rows && total_size)
                {
                    updateRowsProgressApprox(
                        *this, chunk, total_size, total_rows_approx_accumulated, total_rows_count_times, total_rows_approx_max);
                }

                for (const auto & virtual_column : requested_virtual_columns)
                {
                    if (virtual_column.name == "_path")
                    {
                        chunk.addColumn(virtual_column.type->createColumnConst(num_rows, file_path)->convertToFullColumnIfConst());
                    }
                    else if (virtual_column.name == "_file")
                    {
                        size_t last_slash_pos = file_path.find_last_of('/');
                        auto column = virtual_column.type->createColumnConst(num_rows, file_path.substr(last_slash_pos + 1));
                        chunk.addColumn(column->convertToFullColumnIfConst());
                    }
                }
                queue_holder->setFileProcessed(file_path);
                applyActionAfterProcessing(file_path);
                return chunk;
            }
        }
        catch (const Exception & e)
        {
            LOG_ERROR(log, "Exception in chunk pulling: {} ", e.displayText());
            const auto & failed_file_path = reader.getPath();
            queue_holder->setFileFailed(failed_file_path);
        }


        assert(reader_future.valid());
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

void StorageS3QueueSource::applyActionAfterProcessing(const String & file_path)
{
    LOG_WARNING(log, "Delete {} Bucket {}", file_path, bucket);
    S3::DeleteObjectRequest request;
    request.SetBucket(bucket);
    request.SetKey(file_path);
    auto outcome = client->DeleteObject(request);
    if (!outcome.IsSuccess() && !S3::isNotFoundError(outcome.GetError().GetErrorType()))
    {
        const auto & err = outcome.GetError();
        LOG_ERROR(log, "{} (Code: {})", err.GetMessage(), static_cast<size_t>(err.GetErrorType()));
    }
    else
    {
        LOG_TRACE(log, "Object with path {} was removed from S3", file_path);
    }
}


}

#endif
