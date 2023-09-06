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
#    include <Storages/S3Queue/S3QueueSource.h>
#    include <Storages/StorageS3.h>
#    include <Storages/StorageS3Settings.h>
#    include <Storages/VirtualColumnUtils.h>

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


StorageS3QueueSource::QueueGlobIterator::QueueGlobIterator(
    const S3::Client & client_,
    const S3::URI & globbed_uri_,
    ASTPtr query,
    const NamesAndTypesList & virtual_columns,
    ContextPtr context,
    UInt64 & max_poll_size_,
    const S3Settings::RequestSettings & request_settings_)
    : max_poll_size(max_poll_size_)
    , glob_iterator(std::make_unique<StorageS3QueueSource::DisclosedGlobIterator>(
          client_, globbed_uri_, query, virtual_columns, context, nullptr, request_settings_))
{
    /// todo(kssenii): remove this loop, it should not be here
    while (true)
    {
        KeyWithInfo val = glob_iterator->next();
        if (val.key.empty())
            break;
        keys_buf.push_back(val);
    }
}

Strings StorageS3QueueSource::QueueGlobIterator::filterProcessingFiles(
    const S3QueueMode & engine_mode, std::unordered_set<String> & exclude_keys, const String & max_file)
{
    for (const KeyWithInfo & val : keys_buf)
    {
        auto full_path = val.key;
        if (exclude_keys.find(full_path) != exclude_keys.end())
        {
            LOG_TEST(log, "File {} will be skipped, because it was found in exclude files list "
                     "(either already processed or failed to be processed)", val.key);
            continue;
        }

        if ((engine_mode == S3QueueMode::ORDERED) && (full_path.compare(max_file) <= 0))
            continue;

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
    for (const auto & key_info : processing_keys)
        keys.push_back(key_info.key);

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

StorageS3QueueSource::StorageS3QueueSource(
    const ReadFromFormatInfo & info,
    const String & format_,
    String name_,
    ContextPtr context_,
    std::optional<FormatSettings> format_settings_,
    UInt64 max_block_size_,
    const S3Settings::RequestSettings & request_settings_,
    String compression_hint_,
    const std::shared_ptr<const S3::Client> & client_,
    const String & bucket_,
    const String & version_id_,
    const String & url_host_and_port,
    std::shared_ptr<IIterator> file_iterator_,
    std::shared_ptr<S3QueueFilesMetadata> files_metadata_,
    const S3QueueAction & action_,
    const size_t download_thread_num_)
    : ISource(info.source_header)
    , WithContext(context_)
    , name(std::move(name_))
    , bucket(bucket_)
    , version_id(version_id_)
    , format(format_)
    , columns_desc(info.columns_description)
    , request_settings(request_settings_)
    , client(client_)
    , files_metadata(files_metadata_)
    , requested_virtual_columns(info.requested_virtual_columns)
    , requested_columns(info.requested_columns)
    , file_iterator(file_iterator_)
    , action(action_)
{
    internal_source = std::make_shared<StorageS3Source>(
        info,
        format_,
        name_,
        context_,
        format_settings_,
        max_block_size_,
        request_settings_,
        compression_hint_,
        client_,
        bucket_,
        version_id_,
        url_host_and_port,
        file_iterator,
        download_thread_num_,
        false,
        /* query_info */ std::nullopt);
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
    auto file_progress = getContext()->getFileProgressCallback();
    while (true)
    {
        if (isCancelled() || !reader)
        {
            if (reader)
                reader->cancel();
            break;
        }

        Chunk chunk;
        bool success_in_pulling = false;
        try
        {
            if (reader->pull(chunk))
            {
                UInt64 num_rows = chunk.getNumRows();
                auto file_path = reader.getPath();

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
                success_in_pulling = true;
            }
        }
        catch (const Exception & e)
        {
            LOG_ERROR(log, "Exception in chunk pulling: {} ", e.displayText());
            files_metadata->setFileFailed(reader.getFile(), e.message());
            success_in_pulling = false;
        }
        if (success_in_pulling)
        {
            applyActionAfterProcessing(reader.getFile());
            files_metadata->setFileProcessed(reader.getFile());
            return chunk;
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
    switch (action)
    {
        case S3QueueAction::DELETE:
            deleteProcessedObject(file_path);
            break;
        case S3QueueAction::KEEP:
            break;
    }
}

void StorageS3QueueSource::deleteProcessedObject(const String & file_path)
{
    LOG_INFO(log, "Delete processed file {} from bucket {}", file_path, bucket);

    S3::DeleteObjectRequest request;
    request.WithKey(file_path).WithBucket(bucket);
    auto outcome = client->DeleteObject(request);
    if (!outcome.IsSuccess())
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
