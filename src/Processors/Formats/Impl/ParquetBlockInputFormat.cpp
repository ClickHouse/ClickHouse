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
    {
        prepareReader();
        file_reader->set_batch_size(format_settings.parquet.max_block_size);
        std::vector<int> row_group_indices;
        for (int i = 0; i < row_group_total; ++i)
        {
            if (!skip_row_groups.contains(i))
                row_group_indices.emplace_back(i);
        }
        auto read_status = file_reader->GetRecordBatchReader(row_group_indices, column_indices, &current_record_batch_reader);
        if (!read_status.ok())
            throw DB::ParsingException(ErrorCodes::CANNOT_READ_ALL_DATA, "Error while reading Parquet data: {}", read_status.ToString());
    }

    if (is_stopped)
        return {};

    size_t batch_start = getDataOffsetMaybeCompressed(*in);
    auto batch = current_record_batch_reader->Next();
    size_t batch_end = getDataOffsetMaybeCompressed(*in);
    if (batch_start < batch_end)
        approx_bytes_read_for_chunk = batch_end - batch_start;
    else
        approx_bytes_read_for_chunk = 0;

    if (!batch.ok())
    {
        throw ParsingException(ErrorCodes::CANNOT_READ_ALL_DATA, "Error while reading Parquet data: {}",
                               batch.status().ToString());
    }
    if (*batch)
    {
        auto tmp_table = arrow::Table::FromRecordBatches({*batch});
        /// If defaults_for_omitted_fields is true, calculate the default values from default expression for omitted fields.
        /// Otherwise fill the missing columns with zero values of its type.
        BlockMissingValues * block_missing_values_ptr = format_settings.defaults_for_omitted_fields ? &block_missing_values : nullptr;
        arrow_column_to_ch_column->arrowTableToCHChunk(res, *tmp_table, (*tmp_table)->num_rows(), block_missing_values_ptr);
    }
    else
    {
        return {};
    }

    return res;
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
    auto arrow_file = asArrowFile(in, format_settings, is_stopped, "Parquet", PARQUET_MAGIC_BYTES);
    if (is_stopped)
        return;
    THROW_ARROW_NOT_OK(parquet::arrow::OpenFile(std::move(arrow_file), arrow::default_memory_pool(), &file_reader));
    THROW_ARROW_NOT_OK(file_reader->GetSchema(&schema));
}

void ParquetBlockInputFormat::prepareReader()
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

    ArrowFieldIndexUtil<false> field_util(
        format_settings.parquet.case_insensitive_column_matching,
        format_settings.parquet.allow_missing_columns);
    column_indices = field_util.findRequiredIndices(getPort().getHeader(), *schema);
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
