#include <Dictionaries/readInvalidateQuery.h>
#include <DataStreams/IProfilingBlockInputStream.h>

namespace DB
{

namespace ErrorCodes
{
extern const int TOO_MUCH_COLUMNS;
extern const int TOO_MUCH_ROWS;
extern const int RECEIVED_EMPTY_DATA;
}

std::string readInvalidateQuery(IProfilingBlockInputStream & block_input_stream)
{
    block_input_stream.readPrefix();
    std::string response;

    Block block = block_input_stream.read();
    if (!block)
        throw Exception("Empty response", ErrorCodes::RECEIVED_EMPTY_DATA);

    auto columns = block.columns();
    if (columns > 1)
        throw Exception("Expected single column in resultset, got " + std::to_string(columns), ErrorCodes::TOO_MUCH_COLUMNS);

    auto rows = block.rows();
    if (rows == 0)
        throw Exception("Expected single row in resultset, got 0", ErrorCodes::RECEIVED_EMPTY_DATA);
    if (rows > 1)
        throw Exception("Expected single row in resultset, got at least " + std::to_string(rows), ErrorCodes::TOO_MUCH_ROWS);

    auto column = block.getByPosition(0).column;
    response = column->getDataAt(0).toString();

    while ((block = block_input_stream.read()))
    {
        if (block.rows() > 0)
            throw Exception("Expected single row in resultset, got at least " + std::to_string(rows + 1), ErrorCodes::TOO_MUCH_ROWS);
    }

    block_input_stream.readSuffix();

    return response;
}

}
