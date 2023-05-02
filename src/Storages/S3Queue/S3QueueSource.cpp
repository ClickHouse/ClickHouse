#include <algorithm>
#include <Common/ProfileEvents.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include "IO/IOThreadPool.h"
#include "IO/ParallelReadBuffer.h"
#include "Parsers/ASTCreateQuery.h"
#include "config.h"

#if USE_AWS_S3

#    include <Common/isValidUTF8.h>

#    include <Functions/FunctionsConversion.h>

#    include <IO/S3/Requests.h>
#    include <IO/S3Common.h>

#    include <Interpreters/TreeRewriter.h>
#    include <Interpreters/evaluateConstantExpression.h>

#    include <Parsers/ASTFunction.h>
#    include <Parsers/ASTInsertQuery.h>

#    include <Storages/NamedCollectionsHelpers.h>
#    include <Storages/PartitionedSink.h>
#    include <Storages/ReadFromStorageProgress.h>
#    include <Storages/S3Queue/S3QueueSource.h>
#    include <Storages/StorageFactory.h>
#    include <Storages/StorageS3.h>
#    include <Storages/StorageS3Settings.h>
#    include <Storages/StorageSnapshot.h>
#    include <Storages/StorageURL.h>
#    include <Storages/VirtualColumnUtils.h>
#    include <Storages/checkAndGetLiteralArgument.h>
#    include <Storages/getVirtualsForStorage.h>

#    include <Disks/IO/AsynchronousReadIndirectBufferFromRemoteFS.h>
#    include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#    include <Disks/ObjectStorages/StoredObject.h>

#    include <IO/ReadBufferFromS3.h>
#    include <IO/WriteBufferFromS3.h>

#    include <Formats/FormatFactory.h>
#    include <Formats/ReadSchemaUtils.h>

#    include <Processors/Formats/IInputFormat.h>
#    include <Processors/Formats/IOutputFormat.h>
#    include <Processors/Transforms/AddingDefaultsTransform.h>
#    include <QueryPipeline/narrowPipe.h>

#    include <QueryPipeline/QueryPipelineBuilder.h>

#    include <DataTypes/DataTypeString.h>

#    include <aws/core/auth/AWSCredentials.h>

#    include <re2/re2.h>
#    include <Common/CurrentMetrics.h>
#    include <Common/NamedCollections/NamedCollections.h>
#    include <Common/parseGlobs.h>
#    include <Common/quoteString.h>

#    include <filesystem>
#    include <Processors/ISource.h>
#    include <Processors/Sinks/SinkToStorage.h>
#    include <QueryPipeline/Pipe.h>

#    include <boost/algorithm/string.hpp>

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

static const String PARTITION_ID_WILDCARD = "{_partition_id}";

static const std::unordered_set<std::string_view> required_configuration_keys = {
    "url",
};
static const std::unordered_set<std::string_view> optional_configuration_keys
    = {"format",
       "compression",
       "compression_method",
       "structure",
       "access_key_id",
       "secret_access_key",
       "filename",
       "use_environment_credentials",
       "max_single_read_retries",
       "min_upload_part_size",
       "upload_part_size_multiply_factor",
       "upload_part_size_multiply_parts_count_threshold",
       "max_single_part_upload_size",
       "max_connections",
       "expiration_window_seconds",
       "no_sign_request"};

class IOutputFormat;
using OutputFormatPtr = std::shared_ptr<IOutputFormat>;


StorageS3QueueSource::QueueGlobIterator::QueueGlobIterator(
    const S3::Client & client_,
    const S3::URI & globbed_uri_,
    ASTPtr query,
    const Block & virtual_header,
    ContextPtr context,
    StorageS3QueueSource::KeysWithInfo * read_keys_,
    const S3Settings::RequestSettings & request_settings_)
    : bucket(globbed_uri_.bucket)
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

Strings StorageS3QueueSource::QueueGlobIterator::setProcessing(S3QueueMode & engine_mode, std::unordered_set<String> & exclude_keys, const String & max_file)
{
    for (KeyWithInfo val : keys_buf)
    {
        auto full_path = bucket + '/' + val.key;
        if (exclude_keys.find(full_path) != exclude_keys.end())
        {
            LOG_INFO(log, "Found in exclude keys {}", val.key);
            continue;
        }
        if (engine_mode == S3QueueMode::ORDERED && full_path.compare(max_file) <= 0)
        {
            continue;
        }
        if (processing_keys.size() < max_poll_size)
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
            [](const KeyWithInfo & lhs, const KeyWithInfo & rhs) { return lhs.key < rhs.key; });
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
    const S3QueueMode & mode_,
    const S3QueueAction & action_,
    zkutil::ZooKeeperPtr current_zookeeper,
    const String & zookeeper_path_,
    const size_t download_thread_num_)
    : ISource(getHeader(sample_block_, requested_virtual_columns_))
    , WithContext(context_)
    , name(std::move(name_))
    , bucket(bucket_)
    , version_id(version_id_)
    , format(format_)
    , columns_desc(columns_)
    , max_block_size(max_block_size_)
    , request_settings(request_settings_)
    , compression_hint(std::move(compression_hint_))
    , client(client_)
    , sample_block(sample_block_)
    , format_settings(format_settings_)
    , requested_virtual_columns(requested_virtual_columns_)
    , file_iterator(file_iterator_)
    , mode(mode_)
    , action(action_)
    , download_thread_num(download_thread_num_)
    , zookeeper(current_zookeeper)
    , zookeeper_path(zookeeper_path_)
    , create_reader_pool(CurrentMetrics::StorageS3Threads, CurrentMetrics::StorageS3ThreadsActive, 1)
    , create_reader_scheduler(threadPoolCallbackRunner<ReaderHolder>(create_reader_pool, "CreateS3QReader"))
{
    reader = createReader();
    if (reader)
        reader_future = createReaderAsync();
}

StorageS3QueueSource::ReaderHolder StorageS3QueueSource::createReader()
{
    auto [current_key, info] = (*file_iterator)();
    if (current_key.empty())
        return {};

    size_t object_size = info ? info->size : S3::getObjectSize(*client, bucket, current_key, version_id, request_settings);
    auto compression_method = chooseCompressionMethod(current_key, compression_hint);

    InputFormatPtr input_format;
    std::unique_ptr<ReadBuffer> owned_read_buf;

    auto read_buf_or_factory = createS3ReadBuffer(current_key, object_size);
    if (read_buf_or_factory.buf_factory)
    {
        input_format = FormatFactory::instance().getInputRandomAccess(
            format,
            std::move(read_buf_or_factory.buf_factory),
            sample_block,
            getContext(),
            max_block_size,
            /* is_remote_fs */ true,
            compression_method,
            format_settings);
    }
    else
    {
        owned_read_buf = wrapReadBufferWithCompressionMethod(
            std::move(read_buf_or_factory.buf), compression_method, static_cast<int>(getContext()->getSettingsRef().zstd_window_log_max));
        input_format
            = FormatFactory::instance().getInput(format, *owned_read_buf, sample_block, getContext(), max_block_size, format_settings);
    }

    QueryPipelineBuilder builder;
    builder.init(Pipe(input_format));

    if (columns_desc.hasDefaults())
    {
        builder.addSimpleTransform(
            [&](const Block & header)
            { return std::make_shared<AddingDefaultsTransform>(header, columns_desc, *input_format, getContext()); });
    }

    auto pipeline = std::make_unique<QueryPipeline>(QueryPipelineBuilder::getPipeline(std::move(builder)));
    auto current_reader = std::make_unique<PullingPipelineExecutor>(*pipeline);

    return ReaderHolder{fs::path(bucket) / current_key, std::move(owned_read_buf), std::move(pipeline), std::move(current_reader)};
}

std::future<StorageS3QueueSource::ReaderHolder> StorageS3QueueSource::createReaderAsync()
{
    return create_reader_scheduler([this] { return createReader(); }, 0);
}

StorageS3QueueSource::ReadBufferOrFactory StorageS3QueueSource::createS3ReadBuffer(const String & key, size_t object_size)
{
    auto read_settings = getContext()->getReadSettings().adjustBufferSize(object_size);
    read_settings.enable_filesystem_cache = false;
    auto download_buffer_size = getContext()->getSettings().max_download_buffer_size;
    const bool object_too_small = object_size <= 2 * download_buffer_size;

    // Create a read buffer that will prefetch the first ~1 MB of the file.
    // When reading lots of tiny files, this prefetching almost doubles the throughput.
    // For bigger files, parallel reading is more useful.
    if (object_too_small && read_settings.remote_fs_method == RemoteFSReadMethod::threadpool)
    {
        LOG_TRACE(log, "Downloading object of size {} from S3 with initial prefetch", object_size);
        return {.buf = createAsyncS3ReadBuffer(key, read_settings, object_size)};
    }

    auto factory = std::make_unique<ReadBufferS3Factory>(client, bucket, key, version_id, object_size, request_settings, read_settings);
    return {.buf_factory = std::move(factory)};
}

std::unique_ptr<ReadBuffer>
StorageS3QueueSource::createAsyncS3ReadBuffer(const String & key, const ReadSettings & read_settings, size_t object_size)
{
    auto read_buffer_creator =
        [this, read_settings, object_size](const std::string & path, size_t read_until_position) -> std::shared_ptr<ReadBufferFromFileBase>
    {
        return std::make_shared<ReadBufferFromS3>(
            client,
            bucket,
            path,
            version_id,
            request_settings,
            read_settings,
            /* use_external_buffer */ true,
            /* offset */ 0,
            read_until_position,
            /* restricted_seek */ true,
            object_size);
    };

    auto s3_impl = std::make_unique<ReadBufferFromRemoteFSGather>(
        std::move(read_buffer_creator), StoredObjects{StoredObject{key, object_size}}, read_settings);

    auto & pool_reader = getContext()->getThreadPoolReader(Context::FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER);
    auto async_reader = std::make_unique<AsynchronousReadIndirectBufferFromRemoteFS>(pool_reader, read_settings, std::move(s3_impl));

    async_reader->setReadUntilEnd();
    if (read_settings.remote_fs_prefetch)
        async_reader->prefetch(DEFAULT_PREFETCH_PRIORITY);

    return async_reader;
}

StorageS3QueueSource::~StorageS3QueueSource()
{
    create_reader_pool.wait();
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
        LOG_WARNING(log, "Try to pull new chunk");
        try
        {
            if (reader->pull(chunk))
            {
                LOG_WARNING(log, "Success in pulling!");
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
                LOG_WARNING(log, "Set processed: {}", file_path);
                setFileProcessed(file_path);
                applyActionAfterProcessing(file_path);
                return chunk;
            }
        }
        catch (const Exception & e)
        {
            LOG_ERROR(log, "Exception: {} ", e.displayText());
            const auto & failed_file_path = reader.getPath();
            LOG_WARNING(log, "Set failed: {}", failed_file_path);
            setFileFailed(failed_file_path);
        }


        assert(reader_future.valid());
        reader = reader_future.get();

        if (!reader)
            break;

        /// Even if task is finished the thread may be not freed in pool.
        /// So wait until it will be freed before scheduling a new task.
        create_reader_pool.wait();
        reader_future = createReaderAsync();
    }

    return {};
}

void StorageS3QueueSource::setFileProcessed(const String & file_path)
{
    std::lock_guard lock(mutex);
    if (mode == S3QueueMode::UNORDERED)
    {
        String processed_files = zookeeper->get(zookeeper_path + "/processed");
        std::unordered_set<String> processed = parseCollection(processed_files);

        processed.insert(file_path);
        Strings set_processed;
        set_processed.insert(set_processed.end(), processed.begin(), processed.end());

        zookeeper->set(zookeeper_path + "/processed", toString(set_processed));
    }
    else
    {
        zookeeper->set(zookeeper_path + "/processed", file_path);
    }
}


void StorageS3QueueSource::setFileFailed(const String & file_path)
{
    std::lock_guard lock(mutex);
    String processed_files = zookeeper->get(zookeeper_path + "/failed");
    std::unordered_set<String> processed = parseCollection(processed_files);

    processed.insert(file_path);
    Strings set_failed;
    set_failed.insert(set_failed.end(), processed.begin(), processed.end());

    zookeeper->set(zookeeper_path + "/failed", toString(set_failed));
}


void StorageS3QueueSource::applyActionAfterProcessing(const String & file_path)
{
    LOG_WARNING(log, "Delete {} Bucke {}", file_path, bucket);
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

std::unordered_set<String> StorageS3QueueSource::parseCollection(String & files)
{
    ReadBuffer rb(const_cast<char *>(reinterpret_cast<const char *>(files.data())), files.length(), 0);
    Strings deserialized;
    try
    {
        readQuoted(deserialized, rb);
    }
    catch (...)
    {
        deserialized = {};
    }

    std::unordered_set<String> processed(deserialized.begin(), deserialized.end());

    return processed;
}


}

#endif
