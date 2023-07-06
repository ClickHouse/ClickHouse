#include "ORCBlockInputFormat.h"
#include <boost/algorithm/string/case_conv.hpp>
#if USE_ORC

#include <Formats/FormatFactory.h>
#include <Formats/SchemaInferenceUtils.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
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

ORCBlockInputFormat::ORCBlockInputFormat(ReadBuffer & in_, Block header_, const FormatSettings & format_settings_)
    : IInputFormat(std::move(header_), &in_), format_settings(format_settings_), skip_stripes(format_settings.orc.skip_stripes)
{
}

Chunk ORCBlockInputFormat::generate()
{
    block_missing_values.clear();

    if (!file_reader)
        prepareFileReader();

    if (!stripe_reader)
    {
        if (!prepareStripeReader())
            return {};
    }

    if (is_stopped)
        return {};

    std::shared_ptr<arrow::RecordBatch> batch;
    while (true)
    {
        /// Read from batch reader
        auto batch_result = stripe_reader->Next();
        if (!batch_result.ok())
            throw ParsingException(
                ErrorCodes::CANNOT_READ_ALL_DATA, "Failed to read from batch reader: {}", batch_result.status().ToString());

        batch = std::move(batch_result).ValueOrDie();
        if (batch)
            break;

        /// No more batches in current stripe, prepare next stripe
        if (!prepareStripeReader())
            return {};
    }

    std::cout << "generate " << batch->num_rows() << " rows" << std::endl;

    auto table_result = arrow::Table::FromRecordBatches({batch});
    if (!table_result.ok())
        throw ParsingException(
            ErrorCodes::CANNOT_READ_ALL_DATA, "Error while reading batch of ORC data: {}", table_result.status().ToString());

    /// We should extract the number of rows directly from the stripe, because in case when
    /// record batch contains 0 columns (for example if we requested only columns that
    /// are not presented in data) the number of rows in record batch will be 0.
    auto table = std::move(table_result).ValueOrDie();
    if (!table)
        return {};

    size_t num_rows = batch->num_rows();
    approx_bytes_read_for_chunk = num_rows * current_stripe_info.length / current_stripe_info.num_rows;
    std::cout << "approx_bytes_read_for_chunk:" << approx_bytes_read_for_chunk << std::endl;

    Chunk res;
    /// If defaults_for_omitted_fields is true, calculate the default values from default expression for omitted fields.
    /// Otherwise fill the missing columns with zero values of its type.
    BlockMissingValues * block_missing_values_ptr = format_settings.defaults_for_omitted_fields ? &block_missing_values : nullptr;
    arrow_column_to_ch_column->arrowTableToCHChunk(res, table, num_rows, block_missing_values_ptr);
    return res;
}

void ORCBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();

    file_reader.reset();
    stripe_reader.reset();
    include_indices.clear();
    block_missing_values.clear();
}

const BlockMissingValues & ORCBlockInputFormat::getMissingValues() const
{
    return block_missing_values;
}


static void getFileReaderAndSchema(
    ReadBuffer & in,
    std::unique_ptr<arrow::adapters::orc::ORCFileReader> & file_reader,
    std::shared_ptr<arrow::Schema> & schema,
    const FormatSettings & format_settings,
    std::atomic<int> & is_stopped)
{
    auto arrow_file = asArrowFile(in, format_settings, is_stopped, "ORC", ORC_MAGIC_BYTES);
    if (is_stopped)
        return;

    auto result = arrow::adapters::orc::ORCFileReader::Open(arrow_file, arrow::default_memory_pool());
    if (!result.ok())
        throw Exception::createDeprecated(result.status().ToString(), ErrorCodes::BAD_ARGUMENTS);
    file_reader = std::move(result).ValueOrDie();

    auto read_schema_result = file_reader->ReadSchema();
    if (!read_schema_result.ok())
        throw Exception::createDeprecated(read_schema_result.status().ToString(), ErrorCodes::BAD_ARGUMENTS);
    schema = std::move(read_schema_result).ValueOrDie();
}

void ORCBlockInputFormat::prepareFileReader()
{
    std::shared_ptr<arrow::Schema> schema;
    getFileReaderAndSchema(*in, file_reader, schema, format_settings, is_stopped);
    if (is_stopped)
        return;

    total_stripes = static_cast<int>(file_reader->NumberOfStripes());
    current_stripe = -1;

    arrow_column_to_ch_column = std::make_unique<ArrowColumnToCHColumn>(
        getPort().getHeader(),
        "ORC",
        format_settings.orc.import_nested,
        format_settings.orc.allow_missing_columns,
        format_settings.null_as_default,
        format_settings.orc.case_insensitive_column_matching);

    const bool ignore_case = format_settings.orc.case_insensitive_column_matching;
    std::unordered_set<String> nested_table_names;
    if (format_settings.orc.import_nested)
        nested_table_names = Nested::getAllTableNames(getPort().getHeader(), ignore_case);

    for (int i = 0; i < schema->num_fields(); ++i)
    {
        const auto & name = schema->field(i)->name();
        if (getPort().getHeader().has(name, ignore_case) || nested_table_names.contains(ignore_case ? boost::to_lower_copy(name) : name))
            include_indices.push_back(i);
    }
}

bool ORCBlockInputFormat::prepareStripeReader()
{
    assert(file_reader);

    // std::cout << "prepare stripe reader" << std::endl;

    ++current_stripe;
    for (; current_stripe < total_stripes && skip_stripes.contains(current_stripe); ++current_stripe)
        ;

    /// No more stripes to read
    if (current_stripe >= total_stripes)
    {
        // std::cout << "current_stripe:" << current_stripe << " total_stripes:" << total_stripes << std::endl;
        // std::cout << "no more stripes to read" << std::endl;
        return false;
    }

    /// Seek to current stripe
    current_stripe_info = file_reader->GetStripeInformation(current_stripe);
    auto seek_result = file_reader->Seek(current_stripe_info.first_row_id);
    if (!seek_result.ok())
        throw ParsingException(
            ErrorCodes::CANNOT_READ_ALL_DATA, "Failed to seek stripe {}: {}", current_stripe, seek_result.ToString());

    /// Create batch reader for current stripe
    auto result = file_reader->NextStripeReader(format_settings.orc.row_batch_size, include_indices);
    if (!result.ok())
        throw ParsingException(ErrorCodes::CANNOT_READ_ALL_DATA, "Failed to create batch reader: {}", result.status().ToString());

    /// No more stripes to read
    stripe_reader = std::move(result).ValueOrDie();
    if (!stripe_reader)
    {
        std::cout << "no more stripes to read2" << std::endl;
        return false;
    }

    std::cout << "prepare stripe reader finish" << std::endl;
    return true;
}

ORCSchemaReader::ORCSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : ISchemaReader(in_), format_settings(format_settings_)
{
}

NamesAndTypesList ORCSchemaReader::readSchema()
{
    std::unique_ptr<arrow::adapters::orc::ORCFileReader> file_reader;
    std::shared_ptr<arrow::Schema> schema;
    std::atomic<int> is_stopped = 0;
    getFileReaderAndSchema(in, file_reader, schema, format_settings, is_stopped);
    auto header = ArrowColumnToCHColumn::arrowSchemaToCHHeader(
        *schema, "ORC", format_settings.orc.skip_columns_with_unsupported_types_in_schema_inference);
    if (format_settings.schema_inference_make_columns_nullable)
        return getNamesAndRecursivelyNullableTypes(header);
    return header.getNamesAndTypesList();}

void registerInputFormatORC(FormatFactory & factory)
{
    factory.registerInputFormat(
            "ORC",
            [](ReadBuffer &buf,
                const Block &sample,
                const RowInputFormatParams &,
                const FormatSettings & settings)
            {
                return std::make_shared<ORCBlockInputFormat>(buf, sample, settings);
            });
    factory.markFormatSupportsSubcolumns("ORC");
    factory.markFormatSupportsSubsetOfColumns("ORC");
}

void registerORCSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "ORC",
        [](ReadBuffer & buf, const FormatSettings & settings)
        {
            return std::make_shared<ORCSchemaReader>(buf, settings);
        }
        );

    factory.registerAdditionalInfoForSchemaCacheGetter("ORC", [](const FormatSettings & settings)
    {
        return fmt::format("schema_inference_make_columns_nullable={}", settings.schema_inference_make_columns_nullable);
    });
}

}
#else

namespace DB
{
    class FormatFactory;
    void registerInputFormatORC(FormatFactory &)
    {
    }

    void registerORCSchemaReader(FormatFactory &)
    {
    }
}

#endif
