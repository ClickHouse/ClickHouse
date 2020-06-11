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
    prepareReader();
}

Chunk ParquetBlockInputFormat::generate()
{
    Chunk res;
    const Block & header = getPort().getHeader();

    if (row_group_current >= row_group_total)
        return res;

    std::shared_ptr<arrow::Table> table;
    arrow::Status read_status = file_reader->ReadRowGroup(row_group_current, column_indices, &table);
    if (!read_status.ok())
        throw Exception{"Error while reading Parquet data: " + read_status.ToString(),
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
    prepareReader();
}

void ParquetBlockInputFormat::prepareReader()
{
    THROW_ARROW_NOT_OK(parquet::arrow::OpenFile(asArrowFile(in), arrow::default_memory_pool(), &file_reader));
    row_group_total = file_reader->num_row_groups();
    row_group_current = 0;

    std::shared_ptr<arrow::Schema> schema;
    THROW_ARROW_NOT_OK(file_reader->GetSchema(&schema));

    for (int i = 0; i < schema->num_fields(); ++i)
    {
        if (getPort().getHeader().has(schema->field(i)->name()))
        {
            column_indices.push_back(i);
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
