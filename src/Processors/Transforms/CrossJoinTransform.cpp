#include <base/defines.h>
#include <base/types.h>

#include "Common/Exception.h"
#include <Common/logger_useful.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/TableJoin.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Processors/Transforms/CrossJoinTransform.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

CrossJoinAlgorithm::CrossJoinAlgorithm(
    JoinPtr table_join_,
    const Blocks & input_headers,
    size_t max_block_size_)
    : table_join(table_join_)
    , log(getLogger("CrossJoinAlgorithm"))
    , max_block_size(max_block_size_)
{
    if (input_headers.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CrossJoinAlgorithm requires exactly two inputs");

    auto strictness = table_join->getTableJoin().strictness();
    if (strictness != JoinStrictness::Any && strictness != JoinStrictness::All)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "CrossJoinAlgorithm is not implemented for strictness {}", strictness);

    auto kind = table_join->getTableJoin().kind();
    if (!isCrossOrComma(kind))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "CrossJoinAlgorithm is not implemented for kind {}", kind);
}

void CrossJoinAlgorithm::initialize(Inputs inputs)
{
    if (inputs.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Two inputs are required, got {}", inputs.size());

    for (size_t i = 0; i < inputs.size(); ++i)
    {
        consume(inputs[i], i);
    }
}

void CrossJoinAlgorithm::swap() noexcept
{
    isSwapped ^= 1;
}

size_t CrossJoinAlgorithm::getRealIndex(size_t num) const
{
    return num ^ isSwapped;
}

void CrossJoinAlgorithm::consume(Input & input, size_t source_num)
{
    if (input.skip_last_row)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "skip_last_row is not supported");

    if (input.permutation)
        throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "permutation is not supported");

    // chunks[source_num] = std::move(input.chunk);
}

CrossJoinTransform::CrossJoinTransform(
        JoinPtr table_join,
        const Blocks & input_headers,
        const Block & output_header,
        size_t max_block_size,
        UInt64 limit_hint_)
    : IMergingTransform<CrossJoinAlgorithm>(
        input_headers,
        output_header,
        /* have_all_inputs_= */ true,
        limit_hint_,
        /* always_read_till_end_= */ false,
        /* empty_chunk_on_finish_= */ true,
        table_join, input_headers, max_block_size)
    , log(getLogger("CrossJoinTransform"))
{
    LOG_TRACE(log, "Use PasteJoinTransform");
}

void CrossJoinTransform::onFinish() {};

}
