#include <algorithm>

#include <base/defines.h>
#include <base/types.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/TableJoin.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Processors/Transforms/CrossJoinTransform.h>
#include "Common/Exception.h"
#include <Common/logger_useful.h>
#include "Core/Joins.h"
#include "Processors/Chunk.h"

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
}

CrossJoinAlgorithm::CrossJoinAlgorithm(JoinPtr table_join_, const Blocks & input_headers)
    : table_join(table_join_)
    , log(getLogger("CrossJoinAlgorithm"))
    , max_bytes_to_swap_order(table_join_->getTableJoin().crossJoinMaxBytesToSwapOrder())
    , max_joined_block_rows(table_join_->getTableJoin().maxJoinedBlockRows())
{
    if (input_headers.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CrossJoinAlgorithm requires exactly two inputs");
    for (int i = 0; i < 2; ++i)
        headers[i] = input_headers[i].cloneEmpty();

    auto strictness = table_join->getTableJoin().strictness();
    if (strictness != JoinStrictness::Any && strictness != JoinStrictness::All && strictness != JoinStrictness::Unspecified)
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
        consume(inputs[i], i);
}

void CrossJoinAlgorithm::changeOrder() noexcept
{
    std::swap(left_table_num, right_table_num);
}

void CrossJoinAlgorithm::consume(Input & input, size_t source_num)
{
    if (input.skip_last_row)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "skip_last_row is not supported");

    if (input.permutation)
        throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "permutation is not supported");

    sides[source_num].addChunk(input.chunk);
    if (state == CrossJoinState::MinimumBelowThreshold && sides[source_num].isFull())
    {
        /// It means we find out that one table is small enough
        if (source_num == left_table_num)
        {
            /// Small block should be right
            changeOrder();
        }
        state = CrossJoinState::ReadLeftTableAndJoin;
    }
}

Chunk CrossJoinAlgorithm::getRes(Chunk & left_table_res, Chunk & right_table_res) const
{
    Chunk res;
    if (left_table_num > right_table_num)
        std::swap(left_table_res, right_table_res);
    res = std::move(left_table_res);
    Columns right_columns = right_table_res.detachColumns();
    for (auto & right_column : right_columns)
        res.addColumn(right_column);
    return res;
}

IMergingAlgorithm::Status CrossJoinAlgorithm::merge()
{
    if (state == CrossJoinState::MinimumBelowThreshold)
    {
        UInt64 bytes_in_left_table = sides[left_table_num].bytesInChunks();
        UInt64 bytes_in_right_table = sides[right_table_num].bytesInChunks();
        if (std::min(bytes_in_left_table, bytes_in_right_table) > max_bytes_to_swap_order)
            state = CrossJoinState::ReadRightTable;
        else if (bytes_in_left_table < bytes_in_right_table)
            return Status(left_table_num);
        else
            return Status(right_table_num);
    }
    if (state == CrossJoinState::ReadRightTable)
    {
        if (sides[right_table_num].isFull())
            state = CrossJoinState::ReadLeftTableAndJoin;
        else
            return Status(right_table_num);
    }
    assert(state == CrossJoinState::ReadLeftTableAndJoin);
    assert(sides[right_table_num].isFull());
    /// We have joined already
    if (sides[left_table_num].isFull() && sides[left_table_num].isEmpty())
        return Status({}, true);
    if (sides[left_table_num].isEmpty())
        return Status(left_table_num);

    UInt32 rows_added = 0;
    Chunk left_table_res(headers[left_table_num].getColumns(), 0);
    Chunk right_table_res(headers[right_table_num].getColumns(), 0);
    while (rows_added <= max_joined_block_rows && !sides[left_table_num].isEmpty())
    {
        auto left_chunk = sides[left_table_num].getChunk();
        UInt64 rows_consumed = 0;
        UInt64 rows_in_left_chunk = left_chunk.first.getNumRows();
        const Chunks & right_chunks = sides[right_table_num].getAllChunks();
        while (rows_added <= max_joined_block_rows && left_chunk.second + rows_consumed < rows_in_left_chunk)
        {
            size_t left_row = left_chunk.second + rows_consumed;
            for (; last_consumed_right_chunk < right_chunks.size(); ++last_consumed_right_chunk)
            {
                const Chunk & right_chunk = right_chunks[last_consumed_right_chunk];
                right_table_res.append(right_chunk);
                UInt64 rows_in_right_chunk = right_chunk.getNumRows();
                for (size_t i = 0; i < rows_in_right_chunk; ++i)
                    left_table_res.append(left_chunk.first, left_row, 1);

                rows_added += rows_in_right_chunk;
                if (rows_added > max_joined_block_rows)
                {
                    ++last_consumed_right_chunk;
                    break;
                }
            }

            if (last_consumed_right_chunk == right_chunks.size())
            {
                last_consumed_right_chunk = 0;
                ++rows_consumed;
            }
        }
        sides[left_table_num].consume(rows_consumed);
    }
    return Status(getRes(left_table_res, right_table_res));
}

CrossJoinTransform::CrossJoinTransform(JoinPtr table_join, const Blocks & input_headers, const Block & output_header, UInt64 limit_hint_)
    : IMergingTransform<CrossJoinAlgorithm>(
        input_headers,
        output_header,
        /* have_all_inputs_= */ true,
        limit_hint_,
        /* always_read_till_end_= */ false,
        /* empty_chunk_on_finish_= */ true,
        table_join,
        input_headers)
    , log(getLogger("CrossJoinTransform"))
{
    LOG_TRACE(log, "Use PasteJoinTransform");
}

void CrossJoinTransform::onFinish(){};

}
