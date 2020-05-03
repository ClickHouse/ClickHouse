#include "ArrowBlockInputFormat.h"
#if USE_ARROW

#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <arrow/api.h>
#include <arrow/ipc/reader.h>
#include <arrow/status.h>
#include "ArrowBufferedStreams.h"
#include "ArrowColumnToCHColumn.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_READ_ALL_DATA;
}

ArrowBlockInputFormat::ArrowBlockInputFormat(ReadBuffer & in_, const Block & header_)
    : IInputFormat(header_, in_), arrow_istream{std::make_shared<ArrowBufferedInputStream>(in)}
{
    arrow::Status open_status = arrow::ipc::RecordBatchStreamReader::Open(arrow_istream, &reader);
    if (!open_status.ok())
        throw Exception(open_status.ToString(), ErrorCodes::BAD_ARGUMENTS);
}

Chunk ArrowBlockInputFormat::generate()
{
    Chunk res;

    if (in.eof())
        return res;

    std::vector<std::shared_ptr<arrow::RecordBatch>> single_batch(1);
    arrow::Status read_status = reader->ReadNext(&single_batch[0]);
    if (!read_status.ok())
        throw Exception{"Error while reading batch of Arrow data: " + read_status.ToString(),
                        ErrorCodes::CANNOT_READ_ALL_DATA};

    std::shared_ptr<arrow::Table> table;
    arrow::Status make_status = arrow::Table::FromRecordBatches(single_batch, &table);
    if (!make_status.ok())
        throw Exception{"Error while reading table of Arrow data: " + read_status.ToString(),
                        ErrorCodes::CANNOT_READ_ALL_DATA};

    const Block & header = getPort().getHeader();

    ArrowColumnToCHColumn::arrowTableToCHChunk(res, table, header, "Arrow");

    return res;
}

void ArrowBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();
    reader.reset();
}

void registerInputFormatProcessorArrow(FormatFactory &factory)
{
    factory.registerInputFormatProcessor(
            "Arrow",
            [](ReadBuffer & buf,
               const Block & sample,
               const RowInputFormatParams & /* params */,
               const FormatSettings & /* format_settings */)
            {
                return std::make_shared<ArrowBlockInputFormat>(buf, sample);
            });
}

}
#else

namespace DB
{
class FormatFactory;
void registerInputFormatProcessorArrow(FormatFactory &)
{
}
}

#endif
