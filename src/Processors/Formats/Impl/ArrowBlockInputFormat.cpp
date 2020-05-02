#include "ArrowBlockInputFormat.h"
#if USE_ARROW

#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/status.h>
#include "ArrowColumnToCHColumn.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int BAD_ARGUMENTS;
}


ArrowBlockInputFormat::ArrowBlockInputFormat(ReadBuffer &in_, Block header_) : IInputFormat(std::move(header_), in_)
{
}

Chunk ArrowBlockInputFormat::generate()
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

        std::shared_ptr<arrow::io::RandomAccessFile> in_stream(new arrow::io::BufferReader(*local_buffer));

        arrow::Status open_status = arrow::ipc::RecordBatchFileReader::Open(in_stream, &file_reader);
        if (!open_status.ok())
            return res;

        row_group_total = file_reader->num_record_batches();
        row_group_current = 0;

    } else
        return res;

    if (row_group_current >= row_group_total)
        return res;

    std::vector<std::shared_ptr<arrow::RecordBatch>> singleBatch(1);
    arrow::Status read_status = file_reader->ReadRecordBatch(row_group_current, &singleBatch[0]);

    std::shared_ptr<arrow::Table> table;
    arrow::Status make_status = arrow::Table::FromRecordBatches(singleBatch, &table);
    if (!make_status.ok()) {
        throw Exception{"Cannot make table from record batch", ErrorCodes::CANNOT_READ_ALL_DATA};
    }

    ArrowColumnToCHColumn::arrowTableToCHChunk(res, table, read_status, header, row_group_current, "Arrow");

    return res;
}

void ArrowBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();

    file_reader.reset();
    file_data.clear();
    row_group_total = 0;
    row_group_current = 0;
}

void registerInputFormatProcessorArrow(FormatFactory &factory)
{
    factory.registerInputFormatProcessor(
            "Arrow",
            [](ReadBuffer &buf,
               const Block &sample,
               const RowInputFormatParams & /* params */,
               const FormatSettings & /* settings */)
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
