#include <Dictionaries/readInvalidateQuery.h>

#include <Columns/ColumnSparse.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Dictionaries/DictionaryPipelineExecutor.h>
#include <DataTypes/IDataType.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <IO/WriteBufferFromString.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Formats/FormatSettings.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_MANY_COLUMNS;
    extern const int TOO_MANY_ROWS;
    extern const int RECEIVED_EMPTY_DATA;
}

namespace
{

/// `pull` fills the next block of the result and returns false when there are no more blocks.
template <typename Pull>
std::string readInvalidateQueryImpl(Pull && pull)
{
    Block block;
    while (pull(block))
        if (!block.empty())
            break;

    if (block.empty())
        throw Exception(ErrorCodes::RECEIVED_EMPTY_DATA, "Empty response");

    auto columns = block.columns();
    if (columns > 1)
        throw Exception(ErrorCodes::TOO_MANY_COLUMNS, "Expected single column in resultset, got {}", std::to_string(columns));

    auto rows = block.rows();
    if (rows == 0)
        throw Exception(ErrorCodes::RECEIVED_EMPTY_DATA, "Expected single row in resultset, got 0");
    if (rows > 1)
        throw Exception(ErrorCodes::TOO_MANY_ROWS, "Expected single row in resultset, got at least {}", rows);

    WriteBufferFromOwnString out;
    auto & column_type = block.getByPosition(0);
    /// The default serialization of the declared type requires a column matching that type, so special
    /// representations (e.g. `Sparse` or automatic `LowCardinality` of a `MergeTree` column) are materialized.
    auto column = removeSpecialRepresentations(column_type.column->convertToFullColumnIfConst());
    column_type.type->getDefaultSerialization()->serializeTextQuoted(*column, 0, out, FormatSettings());

    while (pull(block))
        if (block.rows() > 0)
            throw Exception(ErrorCodes::TOO_MANY_ROWS, "Expected single row in resultset, got at least {}", rows + 1);

    return out.str();
}

}

std::string readInvalidateQuery(QueryPipeline & pipeline)
{
    /// The same executor as for dictionary loads, so that invalidation results pass the same normalization boundary.
    DictionaryPipelineExecutor executor(pipeline, /* async */ false);
    return readInvalidateQueryImpl([&](Block & block) { return executor.pull(block); });
}

std::string readInvalidateQuery(RemoteQueryExecutor & executor)
{
    executor.sendQuery();
    auto result = readInvalidateQueryImpl([&](Block & block)
    {
        /// An empty block marks the end of the stream.
        block = executor.readBlock();
        return !block.empty();
    });
    executor.finish();
    return result;
}

}
