#include "ORCBlockInputFormat.h"
#if USE_ORC

#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <arrow/adapters/orc/adapter.h>
#include <arrow/io/memory.h>
#include "ArrowColumnToCHColumn.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_READ_ALL_DATA;
}

ORCBlockInputFormat::ORCBlockInputFormat(ReadBuffer & in_, Block header_) : IInputFormat(std::move(header_), in_)
{
}

Chunk ORCBlockInputFormat::generate()
{
    Chunk res;

    if (in.eof())
        return res;

    file_data.clear();
    {
        WriteBufferFromString file_buffer(file_data);
        copyData(in, file_buffer);
    }

    std::unique_ptr<arrow::Buffer> local_buffer = std::make_unique<arrow::Buffer>(file_data);

    std::shared_ptr<arrow::io::RandomAccessFile> in_stream = std::make_shared<arrow::io::BufferReader>(*local_buffer);

    arrow::Status open_status = arrow::adapters::orc::ORCFileReader::Open(in_stream, arrow::default_memory_pool(), &file_reader);
    if (!open_status.ok())
        throw Exception(open_status.ToString(), ErrorCodes::BAD_ARGUMENTS);

    std::shared_ptr<arrow::Table> table;
    arrow::Status read_status = file_reader->Read(&table);
    if (!read_status.ok())
        throw Exception{"Error while reading ORC data: " + read_status.ToString(),
                        ErrorCodes::CANNOT_READ_ALL_DATA};

    const Block & header = getPort().getHeader();

    ArrowColumnToCHColumn::arrowTableToCHChunk(res, table, header, "ORC");

    return res;
}

void ORCBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();

    file_reader.reset();
    file_data.clear();
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
