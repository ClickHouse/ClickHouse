#include <IO/WriteHelpers.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Processors/Port.h>
#include "Processors/Formats/Impl/JSONEachRowRowOutputFormat.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

IRowOutputFormat::IRowOutputFormat(const Block & header, WriteBuffer & out_)
    : IOutputFormat(header, out_)
    , num_columns(header.columns())
    , types(header.getDataTypes())
    , serializations(header.getSerializations())
{
}

void IRowOutputFormat::consume(DB::Chunk chunk)
{
    auto num_rows = chunk.getNumRows();
    const auto & columns = chunk.getColumns();

    for (size_t row = 0; row < num_rows; ++row)
    {
        if (haveWrittenData())
            writeRowBetweenDelimiter();

        write(columns, row);
        first_row = false;
    }
}

void IRowOutputFormat::consumeTotals(DB::Chunk chunk)
{
    if (!supportTotals())
        return;

    auto num_rows = chunk.getNumRows();
    if (num_rows != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got {} in totals chunk, expected 1", num_rows);

    const auto & columns = chunk.getColumns();

    writeBeforeTotals();
    writeTotals(columns, 0);
    writeAfterTotals();
}

void IRowOutputFormat::consumeExtremes(DB::Chunk chunk)
{
    if (!supportExtremes())
        return;

    auto num_rows = chunk.getNumRows();
    const auto & columns = chunk.getColumns();
    if (num_rows != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Got {} in extremes chunk, expected 2", num_rows);

    writeBeforeExtremes();
    writeMinExtreme(columns, 0);
    writeRowBetweenDelimiter();
    writeMaxExtreme(columns, 1);
    writeAfterExtremes();
}

void IRowOutputFormat::write(const Columns & columns, size_t row_num)
{
    writeRowStartDelimiter();

    for (size_t i = 0; i < num_columns; ++i)
    {
        if (i != 0)
            writeFieldDelimiter();

        writeField(*columns[i], *serializations[i], row_num);
    }

    writeRowEndDelimiter();
}

void IRowOutputFormat::writeMinExtreme(const DB::Columns & columns, size_t row_num)
{
    write(columns, row_num);
}

void IRowOutputFormat::writeMaxExtreme(const DB::Columns & columns, size_t row_num)
{
    write(columns, row_num);
}

void IRowOutputFormat::writeTotals(const DB::Columns & columns, size_t row_num)
{
    write(columns, row_num);
}


class SSEFormat : public IOutputFormat
{
public:
    SSEFormat(WriteBuffer & out_, const Block & header_)
        : IOutputFormat(header_, out_) {}

    String getName() const override { return "SSE"; }

    void consume(Chunk chunk) override
    {
        if (!chunk)
            return;

        WriteBufferFromOwnString json_buffer;
        JSONEachRowRowOutputFormat json_format(json_buffer, getPort(IOutputFormat::PortKind::Main).getHeader(), {});
        json_format.consume(std::move(chunk));
        json_format.finalize();

        out.write("data: ", 6);
        out.write(json_buffer.str().data(), json_buffer.str().size());
        out.write("\n\n", 2);
        out.next();
    }

    void finalizeImpl() override 
    {
        out.write("event: end\n", 11);
        out.write("data: {}\n\n", 10);
        out.next();
    }

    std::string getContentType() const override
    {
        return "text/event-stream";
    }

    void flushImpl() override
    {
        out.next();
    }

    void resetFormatterImpl() override
    {
        out.write("event: reset\n", 12);
        out.write("data: {}\n\n", 10);
        out.next();
    }

    void setRowsBeforeLimit(size_t rows) override
    {
        WriteBufferFromOwnString json_buffer;
        json_buffer.write("{\"rows_before_limit\":", 20);
        writeIntText(rows, json_buffer);
        json_buffer.write("}", 1);
        json_buffer.finalize();

        out.write("event: limit\n", 12);
        out.write("data: ", 6);
        out.write(json_buffer.str().data(), json_buffer.str().size());
        out.write("\n\n", 2);
        out.next();
    }

    void onProgress(const Progress & progress) override
    {
        WriteBufferFromOwnString json_buffer;
        progress.writeJSON(json_buffer);
        json_buffer.finalize();

        out.write("event: progress\n", 15);
        out.write("data: ", 6);
        out.write(json_buffer.str().data(), json_buffer.str().size());
        out.write("\n\n", 2);
        out.next();
    }

    void setException(const String & message) override
    {
        WriteBufferFromOwnString json_buffer;
        json_buffer.write(R"({"error":")", 10);
        json_buffer.write(message.data(), message.size());
        json_buffer.write("\"}", 2);
        json_buffer.finalize();

        out.write("event: error\n", 12);
        out.write("data: ", 6);
        out.write(json_buffer.str().data(), json_buffer.str().size());
        out.write("\n\n", 2);
        out.next();
    }
};

}
