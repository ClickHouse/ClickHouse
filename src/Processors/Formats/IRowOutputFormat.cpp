#include <string>
#include <Processors/Formats/IRowOutputFormat.h>
#include <IO/WriteHelpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

IRowOutputFormat::IRowOutputFormat(const Block & header, WriteBuffer & out_, const Params & params_)
    : IOutputFormat(header, out_)
    , num_columns(header.columns())
    , types(header.getDataTypes())
    , serializations(header.getSerializations())
    , params(params_)
{
}

void IRowOutputFormat::consume(DB::Chunk chunk)
{
    auto num_rows = chunk.getNumRows();
    const auto & columns = chunk.getColumns();

    for (size_t row = 0; row < num_rows; ++row)
    {
        if (!first_row || getRowsReadBefore() != 0)
            writeRowBetweenDelimiter();

        write(columns, row);

        if (params.callback)
            params.callback(columns, row);

        first_row = false;
    }
}

void IRowOutputFormat::consumeTotals(DB::Chunk chunk)
{
    auto num_rows = chunk.getNumRows();
    if (num_rows != 1)
        throw Exception("Got " + toString(num_rows) + " in totals chunk, expected 1", ErrorCodes::LOGICAL_ERROR);

    const auto & columns = chunk.getColumns();

    writeBeforeTotals();
    writeTotals(columns, 0);
    writeAfterTotals();
}

void IRowOutputFormat::consumeExtremes(DB::Chunk chunk)
{
    auto num_rows = chunk.getNumRows();
    const auto & columns = chunk.getColumns();
    if (num_rows != 2)
        throw Exception("Got " + toString(num_rows) + " in extremes chunk, expected 2", ErrorCodes::LOGICAL_ERROR);

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

void IRowOutputFormat::writeMaxExtreme(const DB::Columns & columns, size_t row_num) //-V524
{
    write(columns, row_num);
}

void IRowOutputFormat::writeTotals(const DB::Columns & columns, size_t row_num)
{
    write(columns, row_num);
}

}
