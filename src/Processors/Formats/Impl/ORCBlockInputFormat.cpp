#include "ORCBlockInputFormat.h"
#include <boost/algorithm/string/case_conv.hpp>
#if USE_ORC

#include <Formats/FormatFactory.h>
#include <Formats/ReadSchemaUtils.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include "ArrowBufferedStreams.h"
#include "ArrowColumnToCHColumn.h"
#include <DataTypes/NestedUtils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_READ_ALL_DATA;
}

ORCBlockInputFormat::ORCBlockInputFormat(ReadBuffer & in_, Block header_, const FormatSettings & format_settings_)
    : IInputFormat(std::move(header_), in_), format_settings(format_settings_), skip_stripes(format_settings.orc.skip_stripes)
{
}

Chunk ORCBlockInputFormat::generate()
{
    block_missing_values.clear();

    if (!file_reader)
        prepareReader();

    if (is_stopped)
        return {};

    for (; stripe_current < stripe_total && skip_stripes.contains(stripe_current); ++stripe_current)
        ;

    if (stripe_current >= stripe_total)
        return {};

    auto batch_result = file_reader->ReadStripe(stripe_current, include_indices);
    if (!batch_result.ok())
        throw ParsingException(ErrorCodes::CANNOT_READ_ALL_DATA, "Failed to create batch reader: {}", batch_result.status().ToString());

    auto batch = batch_result.ValueOrDie();
    if (!batch)
        return {};

    auto table_result = arrow::Table::FromRecordBatches({batch});
    if (!table_result.ok())
        throw ParsingException(
            ErrorCodes::CANNOT_READ_ALL_DATA, "Error while reading batch of ORC data: {}", table_result.status().ToString());

    auto table = table_result.ValueOrDie();
    if (!table || !table->num_rows())
        return {};

    ++stripe_current;

    Chunk res;
    arrow_column_to_ch_column->arrowTableToCHChunk(res, table);
    /// If defaults_for_omitted_fields is true, calculate the default values from default expression for omitted fields.
    /// Otherwise fill the missing columns with zero values of its type.
    if (format_settings.defaults_for_omitted_fields)
        for (size_t row_idx = 0; row_idx < res.getNumRows(); ++row_idx)
            for (const auto & column_idx : missing_columns)
                block_missing_values.setBit(column_idx, row_idx);

    return res;
}

void ORCBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();

    file_reader.reset();
    include_indices.clear();
    block_missing_values.clear();
}

const BlockMissingValues & ORCBlockInputFormat::getMissingValues() const
{
    return block_missing_values;
}

static size_t countIndicesForType(std::shared_ptr<arrow::DataType> type)
{
    if (type->id() == arrow::Type::LIST)
        return countIndicesForType(static_cast<arrow::ListType *>(type.get())->value_type()) + 1;

    if (type->id() == arrow::Type::STRUCT)
    {
        int indices = 1;
        auto * struct_type = static_cast<arrow::StructType *>(type.get());
        for (int i = 0; i != struct_type->num_fields(); ++i)
            indices += countIndicesForType(struct_type->field(i)->type());
        return indices;
    }

    if (type->id() == arrow::Type::MAP)
    {
        auto * map_type = static_cast<arrow::MapType *>(type.get());
        return countIndicesForType(map_type->key_type()) + countIndicesForType(map_type->item_type());
    }

    return 1;
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
        throw Exception(result.status().ToString(), ErrorCodes::BAD_ARGUMENTS);
    file_reader = std::move(result).ValueOrDie();

    auto read_schema_result = file_reader->ReadSchema();
    if (!read_schema_result.ok())
        throw Exception(read_schema_result.status().ToString(), ErrorCodes::BAD_ARGUMENTS);
    schema = std::move(read_schema_result).ValueOrDie();
}

void ORCBlockInputFormat::prepareReader()
{
    std::shared_ptr<arrow::Schema> schema;
    getFileReaderAndSchema(*in, file_reader, schema, format_settings, is_stopped);
    if (is_stopped)
        return;

    stripe_total = file_reader->NumberOfStripes();
    stripe_current = 0;

    arrow_column_to_ch_column = std::make_unique<ArrowColumnToCHColumn>(
        getPort().getHeader(),
        "ORC",
        format_settings.orc.import_nested,
        format_settings.orc.allow_missing_columns,
        format_settings.orc.case_insensitive_column_matching);
    missing_columns = arrow_column_to_ch_column->getMissingColumns(*schema);

    const bool ignore_case = format_settings.orc.case_insensitive_column_matching;
    std::unordered_set<String> nested_table_names;
    if (format_settings.orc.import_nested)
        nested_table_names = Nested::getAllTableNames(getPort().getHeader(), ignore_case);

    /// In ReadStripe column indices should be started from 1,
    /// because 0 indicates to select all columns.
    int index = 1;
    for (int i = 0; i < schema->num_fields(); ++i)
    {
        /// LIST type require 2 indices, STRUCT - the number of elements + 1,
        /// so we should recursively count the number of indices we need for this type.
        int indexes_count = countIndicesForType(schema->field(i)->type());
        const auto & name = schema->field(i)->name();
        if (getPort().getHeader().has(name, ignore_case) || nested_table_names.contains(ignore_case ? boost::to_lower_copy(name) : name))
        {
            for (int j = 0; j != indexes_count; ++j)
                include_indices.push_back(index + j);
        }

        index += indexes_count;
    }
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
    return getNamesAndRecursivelyNullableTypes(header);
}

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
    factory.markFormatAsColumnOriented("ORC");
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
