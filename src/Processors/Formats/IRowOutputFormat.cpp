#include <IO/WriteHelpers.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Processors/Port.h>
#include <DataTypes/IDataType.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

IRowOutputFormat::IRowOutputFormat(SharedHeader header, WriteBuffer & out_)
    : IOutputFormat(header, out_)
    , num_columns(header->columns())
    , types(header->getDataTypes())
    , serializations(header->getSerializations())
{
}

void IRowOutputFormat::consume(DB::Chunk chunk)
{
    auto num_rows = chunk.getNumRows();
    const auto & columns = chunk.getColumns();
    updateSerializationsIfNeeded(columns);

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
    updateSerializationsIfNeeded(columns);

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
    updateSerializationsIfNeeded(columns);

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

void IRowOutputFormat::updateSerializationsIfNeeded(const Columns & columns)
{
    if (supportsSpecialSerializationKinds())
    {
        for (size_t i = 0; i != columns.size(); ++i)
            serializations[i] = types[i]->getSerialization(*types[i]->getSerializationInfo(*columns[i]));
    }
}

}
