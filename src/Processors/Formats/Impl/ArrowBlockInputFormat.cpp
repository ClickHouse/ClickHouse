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

ArrowBlockInputFormat::ArrowBlockInputFormat(ReadBuffer & in_, const Block & header_, const FormatSettings & format_settings_)
    : IInputFormat(header_, in_), format_settings{format_settings_}, arrow_istream{std::make_shared<ArrowBufferedInputStream>(in)}
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

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    for (UInt64 batch_i = 0; batch_i < format_settings.arrow.record_batch_size; ++batch_i)
    {
        std::shared_ptr<arrow::RecordBatch> batch;
        arrow::Status read_status = reader->ReadNext(&batch);
        if (!read_status.ok())
            throw Exception{"Error while reading Arrow data: " + read_status.ToString(),
                            ErrorCodes::CANNOT_READ_ALL_DATA};
        // nullptr means `no data left in stream`
        if (!batch)
            break;
        batches.emplace_back(std::move(batch));
    }

    if (batches.empty())
        return res;

    std::shared_ptr<arrow::Table> table;
    arrow::Status make_status = arrow::Table::FromRecordBatches(batches, &table);
    if (!make_status.ok())
        throw Exception{"Error while reading Arrow data: " + make_status.ToString(),
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
               const FormatSettings & format_settings)
            {
                return std::make_shared<ArrowBlockInputFormat>(buf, sample, format_settings);
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
