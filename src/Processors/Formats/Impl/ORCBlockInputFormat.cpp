#include "ORCBlockInputFormat.h"
#if USE_ORC

#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <arrow/adapters/orc/adapter.h>
#include <arrow/io/memory.h>
#include "ArrowBufferedStreams.h"
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
    const Block & header = getPort().getHeader();

    if (in.eof())
        return res;

    arrow::Status open_status = arrow::adapters::orc::ORCFileReader::Open(asArrowFile(in), arrow::default_memory_pool(), &file_reader);
    if (!open_status.ok())
        throw Exception(open_status.ToString(), ErrorCodes::BAD_ARGUMENTS);

    std::shared_ptr<arrow::Table> table;
    arrow::Status read_status = file_reader->Read(&table);
    if (!read_status.ok())
        throw Exception{"Error while reading ORC data: " + read_status.ToString(),
                        ErrorCodes::CANNOT_READ_ALL_DATA};

    ArrowColumnToCHColumn::arrowTableToCHChunk(res, table, header, "ORC");

    return res;
}

void ORCBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();

    file_reader.reset();
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
