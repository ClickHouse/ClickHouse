#include "ParquetBlockInputFormat.h"
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

#include <common/logger_useful.h>

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

ParquetBlockInputFormat::ParquetBlockInputFormat(ReadBuffer & in_, Block header_)
    : IInputFormat(std::move(header_), in_)
{
}

Chunk ParquetBlockInputFormat::generate()
{
    Chunk res;
    const Block & header = getPort().getHeader();

    if (!file_reader)
        prepareReader();

    if (row_group_current >= row_group_total)
        return res;

    std::shared_ptr<arrow::Table> table;
    arrow::Status read_status = file_reader->ReadRowGroup(row_group_current, column_indices, &table);
    if (!read_status.ok())
        throw ParsingException{"Error while reading Parquet data: " + read_status.ToString(),
                        ErrorCodes::CANNOT_READ_ALL_DATA};

    ++row_group_current;

    ArrowColumnToCHColumn::arrowTableToCHChunk(res, table, header, "Parquet");
    return res;
}

void ParquetBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();

    file_reader.reset();
    column_indices.clear();
    row_group_current = 0;
}

static size_t countIndicesForType(std::shared_ptr<arrow::DataType> type)
{
    if (type->id() == arrow::Type::LIST)
        return countIndicesForType(static_cast<arrow::ListType *>(type.get())->value_type());

    int indices = 0;
    if (type->id() == arrow::Type::STRUCT)
    {
        auto * struct_type = static_cast<arrow::StructType *>(type.get());
        for (int i = 0; i != struct_type->num_fields(); ++i)
            indices += countIndicesForType(struct_type->field(i)->type());
    }
    else
        indices = 1;

    return indices;
}

void ParquetBlockInputFormat::prepareReader()
{
    THROW_ARROW_NOT_OK(parquet::arrow::OpenFile(asArrowFile(in), arrow::default_memory_pool(), &file_reader));
    row_group_total = file_reader->num_row_groups();
    row_group_current = 0;

    std::shared_ptr<arrow::Schema> schema;
    THROW_ARROW_NOT_OK(file_reader->GetSchema(&schema));

    int index = 0;
    for (int i = 0; i < schema->num_fields(); ++i)
    {
        if (getPort().getHeader().has(schema->field(i)->name()))
        {
            int indexes_count = countIndicesForType(schema->field(i)->type());
            for (int j = 0; j != indexes_count; ++j)
                column_indices.push_back(index++);
        }
    }
}

void registerInputFormatProcessorParquet(FormatFactory &factory)
{
    factory.registerInputFormatProcessor(
            "Parquet",
            [](ReadBuffer &buf,
                const Block &sample,
                const RowInputFormatParams &,
                const FormatSettings & /* settings */)
            {
                return std::make_shared<ParquetBlockInputFormat>(buf, sample);
            });
    factory.markFormatAsColumnOriented("Parquet");
}

}

#else

namespace DB
{
class FormatFactory;
void registerInputFormatProcessorParquet(FormatFactory &)
{
}
}

#endif
