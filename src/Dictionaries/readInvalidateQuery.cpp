#include "readInvalidateQuery.h"
#include <DataStreams/IBlockInputStream.h>
#include <IO/WriteBufferFromString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_COLUMNS;
    extern const int TOO_MANY_ROWS;
    extern const int RECEIVED_EMPTY_DATA;
}

std::string readInvalidateQuery(IBlockInputStream & block_input_stream)
{
    block_input_stream.readPrefix();

    Block block = block_input_stream.read();
    if (!block)
        throw Exception("Empty response", ErrorCodes::RECEIVED_EMPTY_DATA);

    auto columns = block.columns();
    if (columns > 1)
        throw Exception("Expected single column in resultset, got " + std::to_string(columns), ErrorCodes::TOO_MANY_COLUMNS);

    auto rows = block.rows();
    if (rows == 0)
        throw Exception("Expected single row in resultset, got 0", ErrorCodes::RECEIVED_EMPTY_DATA);
    if (rows > 1)
        throw Exception("Expected single row in resultset, got at least " + std::to_string(rows), ErrorCodes::TOO_MANY_ROWS);

    WriteBufferFromOwnString out;
    auto & column_type = block.getByPosition(0);
    column_type.type->serializeAsTextQuoted(*column_type.column->convertToFullColumnIfConst(), 0, out, FormatSettings());

    while ((block = block_input_stream.read()))
        if (block.rows() > 0)
            throw Exception("Expected single row in resultset, got at least " + std::to_string(rows + 1), ErrorCodes::TOO_MANY_ROWS);

    block_input_stream.readSuffix();
    return out.str();
}

}
