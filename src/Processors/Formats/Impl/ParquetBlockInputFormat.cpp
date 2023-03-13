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
#include <parquet/file_reader.h>
#include "ArrowBufferedStreams.h"
#include "ArrowColumnToCHColumn.h"
#include "ArrowFieldIndexUtil.h"
#include <DataTypes/NestedUtils.h>


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

ParquetBlockInputFormat::ParquetBlockInputFormat(ReadBuffer & in_, Block header_, const FormatSettings & format_settings_)
    : IInputFormat(std::move(header_), in_), format_settings(format_settings_), skip_row_groups(format_settings.parquet.skip_row_groups)
{
}

Chunk ParquetBlockInputFormat::generate()
{
    Chunk res;
    block_missing_values.clear();

    if (!file_reader)
        prepareFileReader();

    while (true)
    {
        if (!current_record_batch_reader && !prepareRowGroupReader())
            return {};
        if (is_stopped)
            return {};

        auto batch = current_record_batch_reader->Next();
        if (!batch.ok())
            throw ParsingException(ErrorCodes::CANNOT_READ_ALL_DATA, "Error while reading Parquet data: {}", batch.status().ToString());
        if (!*batch)
        {
            ++row_group_current;
            current_record_batch_reader.reset();
            continue;
        }

        auto tmp_table = arrow::Table::FromRecordBatches({*batch});
        /// If defaults_for_omitted_fields is true, calculate the default values from default expression for omitted fields.
        /// Otherwise fill the missing columns with zero values of its type.
        BlockMissingValues * block_missing_values_ptr = format_settings.defaults_for_omitted_fields ? &block_missing_values : nullptr;
        arrow_column_to_ch_column->arrowTableToCHChunk(res, *tmp_table, (*tmp_table)->num_rows(), block_missing_values_ptr);

        return res;
    }
}

void ParquetBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();

    file_reader.reset();
    current_record_batch_reader.reset();
    column_indices.clear();
    row_group_current = 0;
    block_missing_values.clear();
}

const BlockMissingValues & ParquetBlockInputFormat::getMissingValues() const
{
    return block_missing_values;
}

static void getFileReaderAndSchema(
    ReadBuffer & in,
    std::unique_ptr<parquet::arrow::FileReader> & file_reader,
    std::shared_ptr<arrow::Schema> & schema,
    const FormatSettings & format_settings,
    std::atomic<int> & is_stopped)
{
    auto arrow_file = asArrowFile(in, format_settings, is_stopped, "Parquet", PARQUET_MAGIC_BYTES, /* avoid_buffering */ true);
    if (is_stopped)
        return;

    parquet::ArrowReaderProperties properties;
    properties.set_use_threads(false);

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
    // With this coalescing, we don't need any readahead on our side, hence avoid_buffering above.
    properties.set_pre_buffer(true);
    auto cache_options = arrow::io::CacheOptions::LazyDefaults();
    cache_options.hole_size_limit = format_settings.parquet.min_bytes_for_seek;
    cache_options.range_size_limit = format_settings.parquet.max_bytes_to_read_at_once;
    properties.set_cache_options(cache_options);

    parquet::arrow::FileReaderBuilder builder;
    THROW_ARROW_NOT_OK(builder.Open(std::move(arrow_file)));
    builder.properties(properties);
    // TODO: Pass custom memory_pool() to enable memory accounting with non-jemalloc allocators.
    THROW_ARROW_NOT_OK(builder.Build(&file_reader));

    THROW_ARROW_NOT_OK(file_reader->GetSchema(&schema));
}

void ParquetBlockInputFormat::prepareFileReader()
{
    std::shared_ptr<arrow::Schema> schema;
    getFileReaderAndSchema(*in, file_reader, schema, format_settings, is_stopped);
    if (is_stopped)
        return;

    row_group_total = file_reader->num_row_groups();
    row_group_current = 0;

    arrow_column_to_ch_column = std::make_unique<ArrowColumnToCHColumn>(
        getPort().getHeader(),
        "Parquet",
        format_settings.parquet.import_nested,
        format_settings.parquet.allow_missing_columns,
        format_settings.null_as_default,
        format_settings.parquet.case_insensitive_column_matching);

    ArrowFieldIndexUtil field_util(
        format_settings.parquet.case_insensitive_column_matching,
        format_settings.parquet.allow_missing_columns);
    column_indices = field_util.findRequiredIndices(getPort().getHeader(), *schema);
}

bool ParquetBlockInputFormat::prepareRowGroupReader()
{
    file_reader->set_batch_size(format_settings.parquet.max_block_size);

    while (row_group_current < row_group_total && skip_row_groups.contains(row_group_current))
        ++row_group_current;
    if (row_group_current >= row_group_total)
        return false;

    // Read row groups one at a time. They're normally hundreds of MB each.
    // If we ever encounter parquet files with lots of tiny row groups, we could detect this here
    // and group them together to reduce number of seeks.

    auto read_status = file_reader->GetRecordBatchReader({row_group_current}, column_indices, &current_record_batch_reader);
    if (!read_status.ok())
        throw DB::ParsingException(ErrorCodes::CANNOT_READ_ALL_DATA, "Error while reading Parquet data: {}", read_status.ToString());

    return true;
}

ParquetSchemaReader::ParquetSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : ISchemaReader(in_), format_settings(format_settings_)
{
}

NamesAndTypesList ParquetSchemaReader::readSchema()
{
    std::unique_ptr<parquet::arrow::FileReader> file_reader;
    std::shared_ptr<arrow::Schema> schema;
    std::atomic<int> is_stopped = 0;
    getFileReaderAndSchema(in, file_reader, schema, format_settings, is_stopped);
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
            [](ReadBuffer &buf,
                const Block &sample,
                const RowInputFormatParams &,
                const FormatSettings & settings)
            {
                return std::make_shared<ParquetBlockInputFormat>(buf, sample, settings);
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
