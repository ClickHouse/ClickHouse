#include "PartitionedSink.h"

#include <Functions/FunctionsConversion.h>

#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/evaluateConstantExpression.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>

#include <Processors/Sources/SourceWithProgress.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_PARSE_TEXT;
}

PartitionedSink::PartitionedSink(
    const ASTPtr & partition_by,
    ContextPtr context_,
    const Block & sample_block_)
    : SinkToStorage(sample_block_)
    , context(context_)
    , sample_block(sample_block_)
{
    std::vector<ASTPtr> arguments(1, partition_by);
    ASTPtr partition_by_string = makeASTFunction(FunctionToString::name, std::move(arguments));

    auto syntax_result = TreeRewriter(context).analyze(partition_by_string, sample_block.getNamesAndTypesList());
    partition_by_expr = ExpressionAnalyzer(partition_by_string, syntax_result, context).getActions(false);
    partition_by_column_name = partition_by_string->getColumnName();
}


SinkPtr PartitionedSink::getSinkForPartition(const String & partition_id)
{
    auto it = sinks.find(partition_id);
    if (it == sinks.end())
    {
        auto sink = createSinkForPartition(partition_id);
        std::tie(it, std::ignore) = sinks.emplace(partition_id, sink);
    }

    return it->second;
}


void PartitionedSink::consume(Chunk chunk)
{
    const auto & columns = chunk.getColumns();

    Block block_with_partition_by_expr = sample_block.cloneWithoutColumns();
    block_with_partition_by_expr.setColumns(columns);
    partition_by_expr->execute(block_with_partition_by_expr);

    const auto * column = block_with_partition_by_expr.getByName(partition_by_column_name).column.get();

    std::unordered_map<String, size_t> sub_chunks_indices;
    IColumn::Selector selector;
    for (size_t row = 0; row < chunk.getNumRows(); ++row)
    {
        auto value = column->getDataAt(row);
        auto [it, inserted] = sub_chunks_indices.emplace(value, sub_chunks_indices.size());
        selector.push_back(it->second);
    }

    Chunks sub_chunks;
    sub_chunks.reserve(sub_chunks_indices.size());
    for (size_t column_index = 0; column_index < columns.size(); ++column_index)
    {
        MutableColumns column_sub_chunks = columns[column_index]->scatter(sub_chunks_indices.size(), selector);
        if (column_index == 0) /// Set sizes for sub-chunks.
        {
            for (const auto & column_sub_chunk : column_sub_chunks)
            {
                sub_chunks.emplace_back(Columns(), column_sub_chunk->size());
            }
        }
        for (size_t sub_chunk_index = 0; sub_chunk_index < column_sub_chunks.size(); ++sub_chunk_index)
        {
            sub_chunks[sub_chunk_index].addColumn(std::move(column_sub_chunks[sub_chunk_index]));
        }
    }

    for (const auto & [partition_id, sub_chunk_index] : sub_chunks_indices)
    {
        getSinkForPartition(partition_id)->consume(std::move(sub_chunks[sub_chunk_index]));
    }
}


void PartitionedSink::onFinish()
{
    for (auto & [partition_id, sink] : sinks)
    {
        sink->onFinish();
    }
}


void PartitionedSink::validatePartitionKey(const String & str, bool allow_slash)
{
    for (const char * i = str.data(); i != str.data() + str.size(); ++i)
    {
        if (static_cast<UInt8>(*i) < 0x20 || *i == '{' || *i == '}' || *i == '*' || *i == '?' || (!allow_slash && *i == '/'))
        {
            /// Need to convert to UInt32 because UInt8 can't be passed to format due to "mixing character types is disallowed".
            UInt32 invalid_char_byte = static_cast<UInt32>(static_cast<UInt8>(*i));
            throw DB::Exception(
                ErrorCodes::CANNOT_PARSE_TEXT, "Illegal character '\\x{:02x}' in partition id starting with '{}'",
                invalid_char_byte, std::string(str.data(), i - str.data()));
        }
    }
}


String PartitionedSink::replaceWildcards(const String & haystack, const String & partition_id)
{
    return boost::replace_all_copy(haystack, PartitionedSink::PARTITION_ID_WILDCARD, partition_id);
}

}
