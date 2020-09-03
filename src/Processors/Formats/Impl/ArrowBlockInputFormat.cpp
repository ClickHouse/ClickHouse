#include "ArrowBlockInputFormat.h"
#if USE_ARROW

#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <arrow/api.h>
#include <arrow/ipc/reader.h>
#include <arrow/result.h>
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
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> batch_result;

    if (stream)
    {
        batch_result = stream_reader->Next();
        if (batch_result.ok() && !(*batch_result))
            return res;
    }
    else
    {
        if (record_batch_current >= record_batch_total)
            return res;

        batch_result = file_reader->ReadRecordBatch(record_batch_current);
    }

    if (!batch_result.ok())
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
            "Error while reading batch of Arrow data: {}", batch_result.status().ToString());

    auto table_result = arrow::Table::FromRecordBatches({*batch_result});
    if (!table_result.ok())
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
            "Error while reading batch of Arrow data: {}", table_result.status().ToString());

    ++record_batch_current;

    ArrowColumnToCHColumn::arrowTableToCHChunk(res, *table_result, header, "Arrow");

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
    if (stream)
    {
        auto stream_reader_status = arrow::ipc::RecordBatchStreamReader::Open(asArrowFile(in));
        if (!stream_reader_status.ok())
            throw Exception(ErrorCodes::UNKNOWN_EXCEPTION,
                "Error while opening a table: {}", stream_reader_status.status().ToString());
        stream_reader = *stream_reader_status;
    }
    else
    {
        auto file_reader_status = arrow::ipc::RecordBatchFileReader::Open(asArrowFile(in));
        if (!file_reader_status.ok())
            throw Exception(ErrorCodes::UNKNOWN_EXCEPTION,
                "Error while opening a table: {}", file_reader_status.status().ToString());
        file_reader = *file_reader_status;
    }

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
