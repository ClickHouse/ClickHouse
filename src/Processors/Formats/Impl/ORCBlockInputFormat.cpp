#include "ORCBlockInputFormat.h"
#include <boost/algorithm/string/case_conv.hpp>
#if USE_ORC

#include <Formats/FormatFactory.h>
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
    : IInputFormat(std::move(header_), in_), format_settings(format_settings_)
{
}

Chunk ORCBlockInputFormat::generate()
{
    Chunk res;
    block_missing_values.clear();

    if (!file_reader)
        prepareReader();

    if (is_stopped)
        return {};

    std::shared_ptr<arrow::RecordBatchReader> batch_reader;
    auto result = file_reader->NextStripeReader(format_settings.orc.row_batch_size, include_indices);
    if (!result.ok())
        throw ParsingException(ErrorCodes::CANNOT_READ_ALL_DATA, "Failed to create batch reader: {}", result.status().ToString());
    batch_reader = std::move(result).ValueOrDie();
    if (!batch_reader)
    {
        return res;
    }

    std::shared_ptr<arrow::Table> table;
    arrow::Status table_status = batch_reader->ReadAll(&table);
    if (!table_status.ok())
        throw ParsingException(ErrorCodes::CANNOT_READ_ALL_DATA, "Error while reading batch of ORC data: {}", table_status.ToString());

    if (!table || !table->num_rows())
        return res;

    if (format_settings.use_lowercase_column_name)
        table = *table->RenameColumns(include_column_names);

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
    include_column_names.clear();
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
    auto arrow_file = asArrowFile(in, format_settings, is_stopped);
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

    if (format_settings.use_lowercase_column_name)
    {
        std::vector<std::shared_ptr<::arrow::Field>> fields;
        fields.reserve(schema->num_fields());
        for (int i = 0; i < schema->num_fields(); ++i)
        {
            const auto& field = schema->field(i);
            auto name = field->name();
            boost::to_lower(name);
            fields.push_back(field->WithName(name));
        }
        schema = arrow::schema(fields, schema->metadata());
    }
}

void ORCBlockInputFormat::prepareReader()
{
    std::shared_ptr<arrow::Schema> schema;
    getFileReaderAndSchema(*in, file_reader, schema, format_settings, is_stopped);
    if (is_stopped)
        return;

    arrow_column_to_ch_column = std::make_unique<ArrowColumnToCHColumn>(
        getPort().getHeader(), "ORC", format_settings.orc.import_nested, format_settings.orc.allow_missing_columns);
    missing_columns = arrow_column_to_ch_column->getMissingColumns(*schema);

    std::unordered_set<String> nested_table_names;
    if (format_settings.orc.import_nested)
        nested_table_names = Nested::getAllTableNames(getPort().getHeader());

    /// In ReadStripe column indices should be started from 1,
    /// because 0 indicates to select all columns.
    int index = 1;
    for (int i = 0; i < schema->num_fields(); ++i)
    {
        /// LIST type require 2 indices, STRUCT - the number of elements + 1,
        /// so we should recursively count the number of indices we need for this type.
        int indexes_count = countIndicesForType(schema->field(i)->type());
        const auto & name = schema->field(i)->name();
        if (getPort().getHeader().has(name) || nested_table_names.contains(name))
        {
            for (int j = 0; j != indexes_count; ++j)
            {
                include_indices.push_back(index + j);
                include_column_names.push_back(name);
            }
        }
        index += indexes_count;
    }
}

ORCSchemaReader::ORCSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_) : ISchemaReader(in_), format_settings(format_settings_)
{
}

NamesAndTypesList ORCSchemaReader::readSchema()
{
    std::unique_ptr<arrow::adapters::orc::ORCFileReader> file_reader;
    std::shared_ptr<arrow::Schema> schema;
    std::atomic<int> is_stopped = 0;
    getFileReaderAndSchema(in, file_reader, schema, format_settings, is_stopped);
    auto header = ArrowColumnToCHColumn::arrowSchemaToCHHeader(*schema, "ORC");
    return header.getNamesAndTypesList();
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
        [](ReadBuffer & buf, const FormatSettings & settings, ContextPtr)
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
