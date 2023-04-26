#include "readInvalidateQuery.h"
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <IO/WriteBufferFromString.h>
#include <Formats/FormatSettings.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_COLUMNS;
    extern const int TOO_MANY_ROWS;
    extern const int RECEIVED_EMPTY_DATA;
}

std::string readInvalidateQuery(QueryPipeline pipeline)
{
    PullingPipelineExecutor executor(pipeline);

    Block block;
    while (executor.pull(block))
        if (block)
            break;

    if (!block)
        throw Exception(ErrorCodes::RECEIVED_EMPTY_DATA, "Empty response");

    auto columns = block.columns();
    if (columns > 1)
        throw Exception(ErrorCodes::TOO_MANY_COLUMNS, "Expected single column in resultset, got {}", std::to_string(columns));

    auto rows = block.rows();
    if (rows == 0)
        throw Exception(ErrorCodes::RECEIVED_EMPTY_DATA, "Expected single row in resultset, got 0");
    if (rows > 1)
        throw Exception(ErrorCodes::TOO_MANY_ROWS, "Expected single row in resultset, got at least {}", std::to_string(rows));

    WriteBufferFromOwnString out;
    auto & column_type = block.getByPosition(0);
    column_type.type->getDefaultSerialization()->serializeTextQuoted(*column_type.column->convertToFullColumnIfConst(), 0, out, FormatSettings());

    while (executor.pull(block))
        if (block.rows() > 0)
            throw Exception(ErrorCodes::TOO_MANY_ROWS, "Expected single row in resultset, got at least {}", std::to_string(rows + 1));

    return out.str();
}

}
