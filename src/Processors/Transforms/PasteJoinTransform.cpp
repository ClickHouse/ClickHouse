#include <cassert>
#include <cstddef>
#include <limits>
#include <memory>
#include <type_traits>

#include <base/defines.h>
#include <base/types.h>

#include <Common/logger_useful.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/TableJoin.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Processors/Transforms/PasteJoinTransform.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

template <bool has_left_nulls, bool has_right_nulls>
int nullableCompareAt(const IColumn & left_column, const IColumn & right_column, size_t lhs_pos, size_t rhs_pos, int null_direction_hint = 1)
{
    if constexpr (has_left_nulls && has_right_nulls)
    {
        const auto * left_nullable = checkAndGetColumn<ColumnNullable>(left_column);
        const auto * right_nullable = checkAndGetColumn<ColumnNullable>(right_column);

        if (left_nullable && right_nullable)
        {
            int res = left_nullable->compareAt(lhs_pos, rhs_pos, right_column, null_direction_hint);
            if (res)
                return res;

            /// NULL != NULL case
            if (left_nullable->isNullAt(lhs_pos))
                return null_direction_hint;

            return 0;
        }
    }

    if constexpr (has_left_nulls)
    {
        if (const auto * left_nullable = checkAndGetColumn<ColumnNullable>(left_column))
        {
            if (left_nullable->isNullAt(lhs_pos))
                return null_direction_hint;
            return left_nullable->getNestedColumn().compareAt(lhs_pos, rhs_pos, right_column, null_direction_hint);
        }
    }

    if constexpr (has_right_nulls)
    {
        if (const auto * right_nullable = checkAndGetColumn<ColumnNullable>(right_column))
        {
            if (right_nullable->isNullAt(rhs_pos))
                return -null_direction_hint;
            return left_column.compareAt(lhs_pos, rhs_pos, right_nullable->getNestedColumn(), null_direction_hint);
        }
    }

    return left_column.compareAt(lhs_pos, rhs_pos, right_column, null_direction_hint);
}

ColumnPtr replicateRow(const IColumn & column, size_t num)
{
    MutableColumnPtr res = column.cloneEmpty();
    res->insertManyFrom(column, 0, num);
    return res;
}

template <typename TColumns>
void copyColumnsResized(const TColumns & cols, size_t start, size_t size, Chunk & result_chunk)
{
    for (const auto & col : cols)
    {
        if (col->empty())
        {
            /// add defaults
            result_chunk.addColumn(col->cloneResized(size));
        }
        else if (col->size() == 1)
        {
            /// copy same row n times
            result_chunk.addColumn(replicateRow(*col, size));
        }
        else
        {
            /// cut column
            assert(start + size <= col->size());
            result_chunk.addColumn(col->cut(start, size));
        }
    }
}

}

PasteJoinAlgorithm::PasteJoinAlgorithm(
    JoinPtr table_join_,
    const Blocks & input_headers,
    size_t max_block_size_)
    : table_join(table_join_)
    , max_block_size(max_block_size_)
    , log(&Poco::Logger::get("PasteJoinAlgorithm"))
{
    if (input_headers.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PasteJoinAlgorithm requires exactly two inputs");

    auto strictness = table_join->getTableJoin().strictness();
    if (strictness != JoinStrictness::Any && strictness != JoinStrictness::All)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "PasteJoinAlgorithm is not implemented for strictness {}", strictness);

    auto kind = table_join->getTableJoin().kind();
    if (!isPaste(kind))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "PasteJoinAlgorithm is not implemented for kind {}", kind);

    for (const auto & [left_key, right_key] : table_join->getTableJoin().leftToRightKeyRemap())
    {
        size_t left_idx = input_headers[0].getPositionByName(left_key);
        size_t right_idx = input_headers[1].getPositionByName(right_key);
        left_to_right_key_remap[left_idx] = right_idx;
    }
}

void PasteJoinAlgorithm::logElapsed(double seconds)
{
    LOG_TRACE(log,
        "Finished pocessing in {} seconds"
        ", left: {} blocks, {} rows; right: {} blocks, {} rows"
        ", max blocks loaded to memory: {}",
        seconds, stat.num_blocks[0], stat.num_rows[0], stat.num_blocks[1], stat.num_rows[1],
        stat.max_blocks_loaded);
}

static void prepareChunk(Chunk & chunk)
{
    if (!chunk)
        return;

    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    for (auto & column : columns)
        column = column->convertToFullColumnIfConst();

    chunk.setColumns(std::move(columns), num_rows);
}

void PasteJoinAlgorithm::initialize(Inputs inputs)
{
    if (inputs.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Two inputs are required, got {}", inputs.size());

    for (size_t i = 0; i < inputs.size(); ++i)
    {
        consume(inputs[i], i);
    }
}

void PasteJoinAlgorithm::consume(Input & input, size_t source_num)
{
    if (input.skip_last_row)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "skip_last_row is not supported");

    if (input.permutation)
        throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "permutation is not supported");

    if (input.chunk)
    {
        stat.num_blocks[source_num] += 1;
        stat.num_rows[source_num] += input.chunk.getNumRows();
    }

    prepareChunk(input.chunk);
    chunks[source_num] = std::move(input.chunk);
}

/// if `source_num == 0` get data from left cursor and fill defaults at right
/// otherwise - vice versa
Chunk PasteJoinAlgorithm::createBlockWithDefaults(size_t source_num, size_t start, size_t num_rows) const
{
    ColumnRawPtrs cols;
    {
        const auto & columns_left = chunks[0].getColumns();
        const auto & columns_right = chunks[1].getColumns();

        for (size_t i = 0; i < columns_left.size(); ++i)
        {
            if (auto it = left_to_right_key_remap.find(i); source_num == 0 || it == left_to_right_key_remap.end())
            {
                cols.push_back(columns_left[i].get());
            }
            else
            {
                cols.push_back(columns_right[it->second].get());
            }
        }

        for (const auto & col : columns_right)
        {
            cols.push_back(col.get());
        }
    }

    Chunk result_chunk;
    copyColumnsResized(cols, start, num_rows, result_chunk);
    return result_chunk;
}

enum ChunkToCut
{
    First,
    Second,
    None,
};

IMergingAlgorithm::Status PasteJoinAlgorithm::merge()
{
    PaddedPODArray<UInt64> indices[2];

    Chunk result;
    for (size_t source_num = 0; source_num < 2; ++source_num)
    {
        ChunkToCut to_cut = None;
        if (chunks[0].getNumRows() != chunks[1].getNumRows())
            to_cut = chunks[0].getNumRows() > chunks[1].getNumRows() ? ChunkToCut::First : ChunkToCut::Second;
        for (const auto & col : chunks[source_num].getColumns())
        {
            if (to_cut == ChunkToCut::First)
                result.addColumn(col->cut(0, chunks[1].getNumRows()));
            else if (to_cut == ChunkToCut::Second)
                result.addColumn(col->cut(0, chunks[0].getNumRows()));
            else
                result.addColumn(col);
        }
    }
    return Status(std::move(result), true);
}

PasteJoinTransform::PasteJoinTransform(
        JoinPtr table_join,
        const Blocks & input_headers,
        const Block & output_header,
        size_t max_block_size,
        UInt64 limit_hint_)
    : IMergingTransform<PasteJoinAlgorithm>(
        input_headers,
        output_header,
        /* have_all_inputs_= */ true,
        limit_hint_,
        /* always_read_till_end_= */ false,
        /* empty_chunk_on_finish_= */ true,
        table_join, input_headers, max_block_size)
    , log(&Poco::Logger::get("PasteJoinTransform"))
{
    LOG_TRACE(log, "Use PasteJoinTransform");
}

void PasteJoinTransform::onFinish()
{
    algorithm.logElapsed(total_stopwatch.elapsedSeconds());
}

}
