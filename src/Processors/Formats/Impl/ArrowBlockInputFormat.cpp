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
    extern const int UNKNOWN_EXCEPTION;
    extern const int CANNOT_READ_ALL_DATA;
}

ArrowBlockInputFormat::ArrowBlockInputFormat(ReadBuffer & in_, const Block & header_, bool stream_)
    : IInputFormat(header_, in_), stream{stream_}
{
    prepareReader();
}

Chunk ArrowBlockInputFormat::generate()
{
    Chunk res;
    const Block & header = getPort().getHeader();
    std::vector<std::shared_ptr<arrow::RecordBatch>> single_batch(1);
    arrow::Status read_status;

    if (stream)
    {
        read_status = stream_reader->ReadNext(&single_batch[0]);
        if (!single_batch[0])
            return res;
    }
    else
    {
        if (record_batch_current >= record_batch_total)
            return res;

        read_status = file_reader->ReadRecordBatch(record_batch_current, &single_batch[0]);
    }

    if (!read_status.ok())
        throw Exception{"Error while reading batch of Arrow data: " + read_status.ToString(),
                        ErrorCodes::CANNOT_READ_ALL_DATA};

    std::shared_ptr<arrow::Table> table;
    arrow::Status make_status = arrow::Table::FromRecordBatches(single_batch, &table);
    if (!make_status.ok())
        throw Exception{"Error while reading table of Arrow data: " + read_status.ToString(),
                        ErrorCodes::CANNOT_READ_ALL_DATA};

    ++record_batch_current;

    ArrowColumnToCHColumn::arrowTableToCHChunk(res, table, header, "Arrow");

    return res;
}

void ArrowBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();

    if (stream)
        stream_reader.reset();
    else
        file_reader.reset();
    prepareReader();
}

void ArrowBlockInputFormat::prepareReader()
{
    arrow::Status status;

    if (stream)
        status = arrow::ipc::RecordBatchStreamReader::Open(asArrowFile(in), &stream_reader);
    else
        status = arrow::ipc::RecordBatchFileReader::Open(asArrowFile(in), &file_reader);

    if (!status.ok())
        throw Exception{"Error while opening a table: " + status.ToString(), ErrorCodes::UNKNOWN_EXCEPTION};

    if (stream)
        record_batch_total = -1;
    else
        record_batch_total = file_reader->num_record_batches();

    record_batch_current = 0;
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
            return std::make_shared<ArrowBlockInputFormat>(buf, sample, false);
        });

    factory.registerInputFormatProcessor(
        "ArrowStream",
        [](ReadBuffer & buf,
           const Block & sample,
           const RowInputFormatParams & /* params */,
           const FormatSettings & /* format_settings */)
        {
            return std::make_shared<ArrowBlockInputFormat>(buf, sample, true);
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
