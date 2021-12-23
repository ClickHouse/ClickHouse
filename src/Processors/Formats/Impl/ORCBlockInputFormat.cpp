#include "ORCBlockInputFormat.h"
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

#define THROW_ARROW_NOT_OK(status)                                     \
    do                                                                 \
    {                                                                  \
        if (::arrow::Status _s = (status); !_s.ok())                   \
            throw Exception(_s.ToString(), ErrorCodes::BAD_ARGUMENTS); \
    } while (false)

ORCBlockInputFormat::ORCBlockInputFormat(ReadBuffer & in_, Block header_, const FormatSettings & format_settings_)
    : IInputFormat(std::move(header_), in_), format_settings(format_settings_)
{
}

Chunk ORCBlockInputFormat::generate()
{
    Chunk res;

    if (!file_reader)
        prepareReader();

    std::shared_ptr<arrow::RecordBatchReader> batch_reader;
    arrow::Status reader_status = file_reader->NextStripeReader(format_settings.orc.row_batch_size, include_indices, &batch_reader);
    if (!reader_status.ok())
        throw ParsingException(ErrorCodes::CANNOT_READ_ALL_DATA, "Failed to create batch reader: {}", reader_status.ToString());
    if (!batch_reader)
        return res;

    std::shared_ptr<arrow::Table> table;
    arrow::Status table_status = batch_reader->ReadAll(&table);
    if (!table_status.ok())
        throw ParsingException(ErrorCodes::CANNOT_READ_ALL_DATA, "Error while reading batch of ORC data: {}", table_status.ToString());

    if (!table || !table->num_rows())
        return res;

    arrow_column_to_ch_column->arrowTableToCHChunk(res, table);

    return res;
}

void ORCBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();

    file_reader.reset();
    include_indices.clear();
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

void ORCBlockInputFormat::prepareReader()
{
    THROW_ARROW_NOT_OK(arrow::adapters::orc::ORCFileReader::Open(asArrowFile(*in, format_settings), arrow::default_memory_pool(), &file_reader));

    std::shared_ptr<arrow::Schema> schema;
    THROW_ARROW_NOT_OK(file_reader->ReadSchema(&schema));

    arrow_column_to_ch_column = std::make_unique<ArrowColumnToCHColumn>(getPort().getHeader(), "ORC", format_settings.orc.import_nested);

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
            column_names.push_back(name);
            for (int j = 0; j != indexes_count; ++j)
                include_indices.push_back(index + j);
        }
        index += indexes_count;
    }
}

void registerInputFormatORC(FormatFactory &factory)
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

}
#else

namespace DB
{
    class FormatFactory;
    void registerInputFormatORC(FormatFactory &)
    {
    }
}

#endif
