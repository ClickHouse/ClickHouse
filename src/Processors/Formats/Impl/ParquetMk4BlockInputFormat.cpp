#include "ParquetMk4BlockInputFormat.h"

#include <Common/ThreadPool.h>
#include <Processors/Formats/Impl/Parquet/SchemaConverter.h>

#if USE_PARQUET

namespace ProfileEvents
{
}

namespace CurrentMetrics
{
    extern const Metric ParquetDecoderThreads;
    extern const Metric ParquetDecoderThreadsActive;
    extern const Metric ParquetDecoderThreadsScheduled;

    extern const Metric ParquetDecoderIOThreads;
    extern const Metric ParquetDecoderIOThreadsActive;
    extern const Metric ParquetDecoderIOThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
}

Parquet::ReadOptions convertReadOptions(const FormatSettings & format_settings)
{
    Parquet::ReadOptions options;
    options.use_bloom_filter = format_settings.parquet.bloom_filter_push_down;
    options.use_row_group_min_max = format_settings.parquet.filter_push_down;
    options.schema_inference_force_nullable = format_settings.schema_inference_make_columns_nullable == 1;
    options.schema_inference_force_not_nullable = format_settings.schema_inference_make_columns_nullable == 0;
    options.null_as_default = format_settings.null_as_default;
    //TODO: take these and other options from settings
    options.use_page_min_max = false;
    options.use_prewhere = false;
    return options;
}

ParquetMk4BlockInputFormat::ParquetMk4BlockInputFormat(
    ReadBuffer & buf,
    const Block & header_,
    const FormatSettings & format_settings_,
    Parquet::SharedParsingThreadPoolPtr thread_pool_,
    size_t min_bytes_for_seek)
    : IInputFormat(header_, &buf)
    , format_settings(format_settings_)
    , read_options(convertReadOptions(format_settings))
    , thread_pool(thread_pool_)
{
    read_options.min_bytes_for_seek = min_bytes_for_seek;
    read_options.bytes_per_read_task = min_bytes_for_seek * 4;
    thread_pool->num_readers += 1;
}

void ParquetMk4BlockInputFormat::initializeIfNeeded()
{
    if (!reader)
    {
        reader.emplace();
        reader->reader.prefetcher.init(in, read_options, thread_pool);
        reader->reader.init(read_options, getPort().getHeader(), key_condition);
        reader->init(thread_pool);
    }
}

ParquetMk4BlockInputFormat::~ParquetMk4BlockInputFormat()
{
    thread_pool->num_readers -= 1;
}

Chunk ParquetMk4BlockInputFormat::read()
{
    initializeIfNeeded();
    return reader->read();
}

void ParquetMk4BlockInputFormat::resetParser()
{
    reader.reset();
    IInputFormat::resetParser();
}

NativeParquetSchemaReader::NativeParquetSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings)
    : ISchemaReader(in_)
    , read_options(convertReadOptions(format_settings))
{
}

void NativeParquetSchemaReader::initializeIfNeeded()
{
    if (initialized)
        return;
    Parquet::Prefetcher prefetcher;
    prefetcher.init(&in, read_options, /*thread_pool*/ nullptr);
    file_metadata = Parquet::Reader::readFileMetaData(prefetcher);
    initialized = true;
}

NamesAndTypesList NativeParquetSchemaReader::readSchema()
{
    initializeIfNeeded();
    Parquet::SchemaConverter schemer(file_metadata, read_options, /*sample_block*/ nullptr);
    return schemer.inferSchema();
}

std::optional<size_t> NativeParquetSchemaReader::readNumberOrRows()
{
    initializeIfNeeded();
    return size_t(file_metadata.num_rows);
}

}

#endif
