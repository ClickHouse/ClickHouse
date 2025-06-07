#include "ParquetV3BlockInputFormat.h"

#include <Common/ThreadPool.h>
#include <Processors/Formats/Impl/Parquet/SchemaConverter.h>
#include <Formats/FormatParserGroup.h>
#include <IO/SharedThreadPools.h>

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
    options.use_page_min_max = format_settings.parquet.page_filter_push_down;
    options.always_use_offset_index = format_settings.parquet.use_offset_index;
    options.case_insensitive_column_matching = format_settings.parquet.case_insensitive_column_matching;
    options.schema_inference_force_nullable = format_settings.schema_inference_make_columns_nullable == 1;
    options.schema_inference_force_not_nullable = format_settings.schema_inference_make_columns_nullable == 0;
    options.null_as_default = format_settings.null_as_default;
    options.max_block_size = format_settings.parquet.max_block_size;
    options.preferred_block_size_bytes = format_settings.parquet.prefer_block_bytes;
    options.fuzz = format_settings.parquet.fuzz;
    return options;
}

ParquetV3BlockInputFormat::ParquetV3BlockInputFormat(
    ReadBuffer & buf,
    const Block & header_,
    const FormatSettings & format_settings_,
    FormatParserGroupPtr parser_group_,
    size_t min_bytes_for_seek)
    : IInputFormat(header_, &buf)
    , format_settings(format_settings_)
    , read_options(convertReadOptions(format_settings))
    , parser_group(parser_group_)
{
    read_options.min_bytes_for_seek = min_bytes_for_seek;
    read_options.bytes_per_read_task = min_bytes_for_seek * 4;
}

void ParquetV3BlockInputFormat::initializeIfNeeded()
{
    if (!reader)
    {
        std::call_once(parser_group->init_flag, [&]
            {
                parser_group->initKeyCondition(getPort().getHeader());

                if (format_settings.parquet.enable_row_group_prefetch && parser_group->max_io_threads > 0)
                    parser_group->io_runner.initThreadPool(
                        getFormatParsingThreadPool().get(), parser_group->max_io_threads, "ParquetPrefetch", CurrentThread::getGroup());

                /// Unfortunately max_parsing_threads setting doesn't have a value for
                /// "do parsing in the same thread as the rest of query processing
                /// (inside IInputFormat::read()), with no thread pool". But such mode seems
                /// useful, at least for testing performance. So we use max_parsing_threads = 1
                /// as a signal to disable thread pool altogether, sacrificing the ability to
                /// use thread pool with 1 thread. We could subtract 1 instead, but then the
                /// by default the thread pool will use `num_cores - 1` threads, also bad.
                if (parser_group->max_parsing_threads <= 1)
                    parser_group->parsing_runner.initManual();
                else
                    parser_group->parsing_runner.initThreadPool(
                        getFormatParsingThreadPool().get(), parser_group->max_parsing_threads, "ParquetDecoder", CurrentThread::getGroup());

                auto ext = std::make_shared<Parquet::ParserGroupExt>();

                if (parser_group->key_condition)
                    parser_group->key_condition->extractSingleColumnConditions(ext->column_conditions, nullptr);

                ext->total_memory_low_watermark = format_settings.parquet.memory_low_watermark;
                ext->total_memory_high_watermark = format_settings.parquet.memory_high_watermark;
                parser_group->opaque = ext;
            });

        reader.emplace();
        reader->reader.prefetcher.init(in, read_options, parser_group);
        reader->reader.init(read_options, getPort().getHeader(), parser_group);
        reader->init(parser_group);
    }
}

Chunk ParquetV3BlockInputFormat::read()
{
    initializeIfNeeded();
    return reader->read();
}

void ParquetV3BlockInputFormat::onCancel() noexcept
{
    if (reader)
        reader->cancel();
}

void ParquetV3BlockInputFormat::resetParser()
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
    prefetcher.init(&in, read_options, /*parser_group_=*/ nullptr);
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
