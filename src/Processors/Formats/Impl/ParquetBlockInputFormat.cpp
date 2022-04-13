#include "ParquetBlockInputFormat.h"
#include <boost/algorithm/string/case_conv.hpp>

#if USE_PARQUET

#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/copyData.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/status.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
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

#define THROW_ARROW_NOT_OK(status)                                     \
    do                                                                 \
    {                                                                  \
        if (::arrow::Status _s = (status); !_s.ok())                   \
            throw Exception(_s.ToString(), ErrorCodes::BAD_ARGUMENTS); \
    } while (false)

ParquetBlockInputFormat::ParquetBlockInputFormat(ReadBuffer & in_, Block header_, const FormatSettings & format_settings_)
    : IInputFormat(std::move(header_), in_), format_settings(format_settings_)
{
}

Chunk ParquetBlockInputFormat::generate()
{
    Chunk res;
    block_missing_values.clear();

    if (!file_reader)
        prepareReader();

    if (is_stopped)
        return {};

    if (row_group_current >= row_group_total)
        return res;

    std::shared_ptr<arrow::Table> table;
    arrow::Status read_status = file_reader->ReadRowGroup(row_group_current, column_indices, &table);
    if (!read_status.ok())
        throw ParsingException{"Error while reading Parquet data: " + read_status.ToString(),
                        ErrorCodes::CANNOT_READ_ALL_DATA};

    if (format_settings.use_lowercase_column_name)
        table = *table->RenameColumns(column_names);

    ++row_group_current;

    arrow_column_to_ch_column->arrowTableToCHChunk(res, table);

    /// If defaults_for_omitted_fields is true, calculate the default values from default expression for omitted fields.
    /// Otherwise fill the missing columns with zero values of its type.
    if (format_settings.defaults_for_omitted_fields)
        for (size_t row_idx = 0; row_idx < res.getNumRows(); ++row_idx)
            for (const auto & column_idx : missing_columns)
                block_missing_values.setBit(column_idx, row_idx);
    return res;
}

void ParquetBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();

    file_reader.reset();
    column_indices.clear();
    column_names.clear();
    row_group_current = 0;
    block_missing_values.clear();
}

const BlockMissingValues & ParquetBlockInputFormat::getMissingValues() const
{
    return block_missing_values;
}

static size_t countIndicesForType(std::shared_ptr<arrow::DataType> type)
{
    if (type->id() == arrow::Type::LIST)
        return countIndicesForType(static_cast<arrow::ListType *>(type.get())->value_type());

    if (type->id() == arrow::Type::STRUCT)
    {
        int indices = 0;
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
    std::unique_ptr<parquet::arrow::FileReader> & file_reader,
    std::shared_ptr<arrow::Schema> & schema,
    const FormatSettings & format_settings,
    std::atomic<int> & is_stopped)
{
    auto arrow_file = asArrowFile(in, format_settings, is_stopped);
    if (is_stopped)
        return;
    THROW_ARROW_NOT_OK(parquet::arrow::OpenFile(std::move(arrow_file), arrow::default_memory_pool(), &file_reader));
    THROW_ARROW_NOT_OK(file_reader->GetSchema(&schema));

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

void ParquetBlockInputFormat::prepareReader()
{
    std::shared_ptr<arrow::Schema> schema;
    getFileReaderAndSchema(*in, file_reader, schema, format_settings, is_stopped);
    if (is_stopped)
        return;

    row_group_total = file_reader->num_row_groups();
    row_group_current = 0;

    arrow_column_to_ch_column = std::make_unique<ArrowColumnToCHColumn>(getPort().getHeader(), "Parquet", format_settings.parquet.import_nested, format_settings.parquet.allow_missing_columns);
    missing_columns = arrow_column_to_ch_column->getMissingColumns(*schema);

    std::unordered_set<String> nested_table_names;
    if (format_settings.parquet.import_nested)
        nested_table_names = Nested::getAllTableNames(getPort().getHeader());

    int index = 0;
    for (int i = 0; i < schema->num_fields(); ++i)
    {
        /// STRUCT type require the number of indexes equal to the number of
        /// nested elements, so we should recursively
        /// count the number of indices we need for this type.
        int indexes_count = countIndicesForType(schema->field(i)->type());
        const auto & name = schema->field(i)->name();
        if (getPort().getHeader().has(name) || nested_table_names.contains(name))
        {
            for (int j = 0; j != indexes_count; ++j)
            {
                column_indices.push_back(index + j);
                column_names.push_back(name);
            }
        }
        index += indexes_count;
    }
}

ParquetSchemaReader::ParquetSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_) : ISchemaReader(in_), format_settings(format_settings_)
{
}

NamesAndTypesList ParquetSchemaReader::readSchema()
{
    std::unique_ptr<parquet::arrow::FileReader> file_reader;
    std::shared_ptr<arrow::Schema> schema;
    std::atomic<int> is_stopped = 0;
    getFileReaderAndSchema(in, file_reader, schema, format_settings, is_stopped);
    auto header = ArrowColumnToCHColumn::arrowSchemaToCHHeader(*schema, "Parquet");
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
    factory.markFormatAsColumnOriented("Parquet");
}

void registerParquetSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "Parquet",
        [](ReadBuffer & buf, const FormatSettings & settings, ContextPtr)
        {
            return std::make_shared<ParquetSchemaReader>(buf, settings);
        }
        );
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
