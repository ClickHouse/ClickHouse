#include "ORCBlockInputFormat.h"
#if USE_ORC

#include <Formats/FormatFactory.h>
#include <IO/BufferBase.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <arrow/io/memory.h>
#include "ArrowColumnToCHColumn.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
}


ORCBlockInputFormat::ORCBlockInputFormat(ReadBuffer & in_, Block header_) : IInputFormat(std::move(header_), in_)
{
}

Chunk ORCBlockInputFormat::generate()
{
    Chunk res;

    const auto & header = getPort().getHeader();

    if (!in.eof())
    {
        if (row_group_current < row_group_total)
            throw Exception{"Got new data, but data from previous chunks was not read " +
                            std::to_string(row_group_current) + "/" + std::to_string(row_group_total),
                            ErrorCodes::CANNOT_READ_ALL_DATA};

        file_data.clear();
        {
            WriteBufferFromString file_buffer(file_data);
            copyData(in, file_buffer);
        }

        std::unique_ptr<arrow::Buffer> local_buffer = std::make_unique<arrow::Buffer>(file_data);

        std::shared_ptr<arrow::io::RandomAccessFile> in_stream = std::make_shared<arrow::io::BufferReader>(*local_buffer);

        bool ok = arrow::adapters::orc::ORCFileReader::Open(in_stream, arrow::default_memory_pool(),
                                                            &file_reader).ok();
        if (!ok)
            return res;

        row_group_total = file_reader->NumberOfRows();
        row_group_current = 0;

    }
    else
        return res;

    if (row_group_current >= row_group_total)
        return res;

    std::shared_ptr<arrow::Table> table;

    arrow::Status read_status = file_reader->Read(&table);

    ArrowColumnToCHColumn::arrowTableToCHChunk(res, table, read_status, header, row_group_current, "ORC");

    return res;
}

void ORCBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();

    file_reader.reset();
    file_data.clear();
    row_group_total = 0;
    row_group_current = 0;
}

void registerInputFormatProcessorORC(FormatFactory &factory)
{
    factory.registerInputFormatProcessor(
            "ORC",
            [](ReadBuffer &buf,
                const Block &sample,
                const RowInputFormatParams &,
                const FormatSettings & /* settings */)
                {
                return std::make_shared<ORCBlockInputFormat>(buf, sample);
            });
}

}
#else

namespace DB
{
    class FormatFactory;
    void registerInputFormatProcessorORC(FormatFactory &)
    {
    }
}

#endif
