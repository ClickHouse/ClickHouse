#include "ParquetBlockInputFormat.h"
#include <boost/algorithm/string/case_conv.hpp>

#if USE_PARQUET

#include <Formats/FormatFactory.h>
#include <Formats/SchemaInferenceUtils.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/copyData.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/status.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/schema.h>
#include <parquet/file_reader.h>
#include "ArrowBufferedStreams.h"
#include "ArrowColumnToCHColumn.h"
#include "ArrowFieldIndexUtil.h"
#include <DataTypes/NestedUtils.h>

namespace CurrentMetrics
{
    extern const Metric ParquetDecoderThreads;
    extern const Metric ParquetDecoderThreadsActive;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_READ_ALL_DATA;
}

#define THROW_ARROW_NOT_OK(status)                                     \
    do                                                                 \
    {                                                                  \
        if (::arrow::Status _s = (status); !_s.ok())                   \
            throw Exception::createDeprecated(_s.ToString(), ErrorCodes::BAD_ARGUMENTS); \
    } while (false)

ParquetBlockInputFormat::ParquetBlockInputFormat(
    ReadBuffer * buf,
    SeekableReadBufferFactoryPtr buf_factory_,
    const Block & header_,
    const FormatSettings & format_settings_,
    size_t max_decoding_threads_,
    size_t min_bytes_for_seek_)
    : IInputFormat(std::move(header_), buf)
    , buf_factory(std::move(buf_factory_))
    , format_settings(format_settings_)
    , skip_row_groups(format_settings.parquet.skip_row_groups)
    , max_decoding_threads(max_decoding_threads_)
    , min_bytes_for_seek(min_bytes_for_seek_)
    , to_deliver(ChunkToDeliver::Compare { .row_group_first = format_settings_.parquet.preserve_order })
{
    if (max_decoding_threads > 1)
        pool.emplace(CurrentMetrics::ParquetDecoderThreads, CurrentMetrics::ParquetDecoderThreadsActive, max_decoding_threads);
}

ParquetBlockInputFormat::~ParquetBlockInputFormat() = default;

void ParquetBlockInputFormat::initializeIfNeeded() {
    if (std::exchange(is_initialized, true))
        return;

    if (buf_factory)
        arrow_file = std::make_shared<RandomAccessFileFromManyReadBuffers>(*buf_factory);
    else
        arrow_file = asArrowFile(*in, format_settings, is_stopped, "Parquet", PARQUET_MAGIC_BYTES, /* avoid_buffering */ true);

    if (is_stopped)
        return;

    metadata = parquet::ReadMetaData(arrow_file);

    std::shared_ptr<arrow::Schema> schema;
    THROW_ARROW_NOT_OK(parquet::arrow::FromParquetSchema(metadata->schema(), &schema));

    row_groups.resize(metadata->num_row_groups());

    ArrowFieldIndexUtil field_util(
        format_settings.parquet.case_insensitive_column_matching,
        format_settings.parquet.allow_missing_columns);
    column_indices = field_util.findRequiredIndices(getPort().getHeader(), *schema);
}

void ParquetBlockInputFormat::prepareRowGroupReader(size_t row_group_idx) {
    auto & row_group = row_groups[row_group_idx];

    parquet::ArrowReaderProperties properties;
    properties.set_use_threads(false);
    properties.set_batch_size(format_settings.parquet.max_block_size);

    // When reading a row group, arrow will:
    //  1. Before reading anything, look at all the byte ranges it'll need to read from the file
    //     (typically one per requested column in the row group). This information is in file
    //     metadata.
    //  2. Coalesce ranges that are close together, trading off seeks vs read amplification.
    //     This is controlled by CacheOptions.
    //  3. Process the columns one by one, issuing the corresponding (coalesced) range reads as
    //     needed. Each range gets its own memory buffer allocated. These buffers stay in memory
    //     (in arrow::io::internal::ReadRangeCache) until the whole row group reading is done.
    //     So the memory usage of a "SELECT *" will be at least the compressed size of a row group
    //     (typically hundreds of MB).
    //
    // With this coalescing, we don't need any readahead on our side, hence avoid_buffering in
    // asArrowFile().
    //
    // This adds one unnecessary copy. We should probably do coalescing and prefetch scheduling on
    // our side instead.
    properties.set_pre_buffer(true);
    auto cache_options = arrow::io::CacheOptions::LazyDefaults();
    cache_options.hole_size_limit = min_bytes_for_seek;
    cache_options.range_size_limit = format_settings.parquet.max_bytes_to_read_at_once;
    properties.set_cache_options(cache_options);

    parquet::arrow::FileReaderBuilder builder;
    THROW_ARROW_NOT_OK(
        builder.Open(arrow_file, /* not to be confused with ArrowReaderProperties */ parquet::default_reader_properties(), metadata));
    builder.properties(properties);
    // TODO: Pass custom memory_pool() to enable memory accounting with non-jemalloc allocators.
    THROW_ARROW_NOT_OK(builder.Build(&row_group.file_reader));

    THROW_ARROW_NOT_OK(
        row_group.file_reader->GetRecordBatchReader({static_cast<int>(row_group_idx)}, column_indices, &row_group.record_batch_reader));

    row_group.arrow_column_to_ch_column = std::make_unique<ArrowColumnToCHColumn>(
        getPort().getHeader(),
        "Parquet",
        format_settings.parquet.import_nested,
        format_settings.parquet.allow_missing_columns,
        format_settings.null_as_default,
        format_settings.parquet.case_insensitive_column_matching);
}

void ParquetBlockInputFormat::scheduleRowGroup(size_t row_group_idx)
{
    chassert(!mutex.try_lock());
    chassert(!row_groups[row_group_idx].running);

    row_groups[row_group_idx].running = true;

    pool->scheduleOrThrowOnError(
        [this, row_group_idx, thread_group = CurrentThread::getGroup()]()
        {
            if (thread_group)
                CurrentThread::attachToGroupIfDetached(thread_group);
            SCOPE_EXIT_SAFE(if (thread_group) CurrentThread::detachFromGroupIfNotDetached(););

            try
            {
                setThreadName("ParquetDecoder");

                threadFunction(row_group_idx);
            }
            catch (...)
            {
                std::lock_guard lock(mutex);
                background_exception = std::current_exception();
                condvar.notify_all();
            }
        });
}

void ParquetBlockInputFormat::threadFunction(size_t row_group_idx)
{
    std::unique_lock lock(mutex);

    auto & row_group = row_groups[row_group_idx];
    chassert(!row_group.done);

    while (row_group.chunks_decoded - row_group.chunks_delivered < max_pending_chunks_per_row_group && !is_stopped)
    {
        if (!decodeOneChunk(row_group_idx, lock))
            break;
    }

    row_group.running = false;
}

bool ParquetBlockInputFormat::decodeOneChunk(size_t row_group_idx, std::unique_lock<std::mutex> & lock)
{
    // TODO: Do most reading on IO threads. Separate max_download_threads from max_parsing_threads.

    auto & row_group = row_groups[row_group_idx];
    chassert(!row_group.done);
    chassert(lock.owns_lock());

    lock.unlock();

    auto end_of_row_group = [&] {
        lock.lock();
        row_group.done = true;
        row_group.arrow_column_to_ch_column.reset();
        row_group.record_batch_reader.reset();
        row_group.file_reader.reset();

        // We may be able to schedule more work now, but can't call scheduleMoreWorkIfNeeded() right
        // here because we're running on the same thread pool, so it may deadlock if thread limit is
        // reached. Wake up generate() instead.
        condvar.notify_all();
    };

    if (!row_group.record_batch_reader)
    {
        if (skip_row_groups.contains(static_cast<int>(row_group_idx)))
        {
            // Pretend that the row group is empty.
            // (This happens kind of late. We could avoid scheduling the row group on a thread in
            // the first place. But the skip_row_groups feature is mostly unused, so it's better to
            // be a little inefficient than to add a bunch of extra mostly-dead code for this.)
            end_of_row_group();
            return false;
        }
        else
        {
            prepareRowGroupReader(row_group_idx);
        }
    }

    auto batch = row_group.record_batch_reader->Next();
    if (!batch.ok())
        throw ParsingException(ErrorCodes::CANNOT_READ_ALL_DATA, "Error while reading Parquet data: {}", batch.status().ToString());

    if (!*batch)
    {
        end_of_row_group();
        return false;
    }

    auto tmp_table = arrow::Table::FromRecordBatches({*batch});

    ChunkToDeliver res = {.chunk_idx = row_group.chunks_decoded, .row_group_idx = row_group_idx};

    /// If defaults_for_omitted_fields is true, calculate the default values from default expression for omitted fields.
    /// Otherwise fill the missing columns with zero values of its type.
    BlockMissingValues * block_missing_values_ptr = format_settings.defaults_for_omitted_fields ? &res.block_missing_values : nullptr;
    row_group.arrow_column_to_ch_column->arrowTableToCHChunk(res.chunk, *tmp_table, (*tmp_table)->num_rows(), block_missing_values_ptr);

    lock.lock();

    ++row_group.chunks_decoded;
    to_deliver.push(std::move(res));
    condvar.notify_all();

    return true;
}

void ParquetBlockInputFormat::scheduleMoreWorkIfNeeded(std::optional<size_t> row_group_touched)
{
    while (row_groups_retired < row_groups.size())
    {
        auto & row_group = row_groups[row_groups_retired];
        if (!row_group.done || row_group.chunks_delivered < row_group.chunks_decoded)
            break;
        ++row_groups_retired;
    }

    if (pool)
    {
        while (row_groups_started - row_groups_retired < max_decoding_threads && row_groups_started < row_groups.size())
            scheduleRowGroup(row_groups_started++);

        if (row_group_touched)
        {
            auto & row_group = row_groups[*row_group_touched];
            if (!row_group.done && !row_group.running && row_group.chunks_decoded - row_group.chunks_delivered < max_pending_chunks_per_row_group)
                scheduleRowGroup(*row_group_touched);
        }
    }
}

Chunk ParquetBlockInputFormat::generate()
{
    initializeIfNeeded();

    std::unique_lock lock(mutex);

    while (true)
    {
        if (background_exception)
        {
            is_stopped = true;
            std::rethrow_exception(background_exception);
        }
        if (is_stopped)
            return {};

        scheduleMoreWorkIfNeeded();

        if (!to_deliver.empty() && (!format_settings.parquet.preserve_order || to_deliver.top().row_group_idx == row_groups_retired))
        {
            ChunkToDeliver chunk = std::move(const_cast<ChunkToDeliver&>(to_deliver.top()));
            to_deliver.pop();

            previous_block_missing_values = chunk.block_missing_values;

            auto & row_group = row_groups[chunk.row_group_idx];
            chassert(chunk.chunk_idx == row_group.chunks_delivered);
            ++row_group.chunks_delivered;

            scheduleMoreWorkIfNeeded(chunk.row_group_idx);

            return std::move(chunk.chunk);
        }

        if (row_groups_retired == row_groups.size())
            return {};

        if (pool)
            condvar.wait(lock);
        else
            decodeOneChunk(row_groups_retired, lock);
    }
}

void ParquetBlockInputFormat::resetParser()
{
    is_stopped = true;
    if (pool)
        pool->wait();

    arrow_file.reset();
    metadata.reset();
    column_indices.clear();
    row_groups.clear();
    while (!to_deliver.empty())
        to_deliver.pop();
    row_groups_retired = 0;
    previous_block_missing_values.clear();
    row_groups_started = 0;
    background_exception = nullptr;

    is_stopped = false;
    is_initialized = false;

    IInputFormat::resetParser();
}

const BlockMissingValues & ParquetBlockInputFormat::getMissingValues() const
{
    return previous_block_missing_values;
}

ParquetSchemaReader::ParquetSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : ISchemaReader(in_), format_settings(format_settings_)
{
}

NamesAndTypesList ParquetSchemaReader::readSchema()
{
    std::atomic<int> is_stopped{0};
    auto file = asArrowFile(in, format_settings, is_stopped, "Parquet", PARQUET_MAGIC_BYTES);

    auto metadata = parquet::ReadMetaData(file);

    std::shared_ptr<arrow::Schema> schema;
    THROW_ARROW_NOT_OK(parquet::arrow::FromParquetSchema(metadata->schema(), &schema));

    auto header = ArrowColumnToCHColumn::arrowSchemaToCHHeader(
        *schema, "Parquet", format_settings.parquet.skip_columns_with_unsupported_types_in_schema_inference);
    if (format_settings.schema_inference_make_columns_nullable)
        return getNamesAndRecursivelyNullableTypes(header);
    return header.getNamesAndTypesList();
}

void registerInputFormatParquet(FormatFactory & factory)
{
    factory.registerInputFormat(
            "Parquet",
            [](ReadBuffer & buf,
               const Block & sample,
               const RowInputFormatParams &,
               const FormatSettings & settings)
            {
                // TODO: Propagate the last two from settings.
                return std::make_shared<ParquetBlockInputFormat>(&buf, nullptr, sample, settings, 1, 4 * DBMS_DEFAULT_BUFFER_SIZE);
            },
            [](SeekableReadBufferFactoryPtr buf_factory,
               const Block & sample,
               const FormatSettings & settings,
               const ReadSettings& read_settings,
               bool is_remote_fs,
               ThreadPoolCallbackRunner<void> /* io_schedule */,
               size_t /* max_download_threads */,
               size_t max_parsing_threads)
            {
                size_t min_bytes_for_seek = is_remote_fs ? read_settings.remote_read_min_bytes_for_seek : 8 * 1024;
                return std::make_shared<ParquetBlockInputFormat>(
                    nullptr,
                    std::move(buf_factory),
                    sample,
                    settings,
                    max_parsing_threads,
                    min_bytes_for_seek);
            });
    factory.markFormatSupportsSubcolumns("Parquet");
    factory.markFormatSupportsSubsetOfColumns("Parquet");
}

void registerParquetSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "Parquet",
        [](ReadBuffer & buf, const FormatSettings & settings)
        {
            return std::make_shared<ParquetSchemaReader>(buf, settings);
        }
        );

    factory.registerAdditionalInfoForSchemaCacheGetter("Parquet", [](const FormatSettings & settings)
    {
        return fmt::format("schema_inference_make_columns_nullable={}", settings.schema_inference_make_columns_nullable);
    });
}

}

#else

namespace DB
{
class FormatFactory;
void registerInputFormatParquet(FormatFactory &)
{
}

void registerParquetSchemaReader(FormatFactory &) {}
}

#endif
