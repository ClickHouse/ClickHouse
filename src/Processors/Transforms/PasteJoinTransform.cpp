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
#include <Processors/Port.h>
#include <Processors/Transforms/PasteJoinTransform.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}


PasteJoinAlgorithm::PasteJoinAlgorithm(
    JoinPtr table_join_,
    const Blocks & input_headers,
    size_t max_block_size_)
    : table_join(table_join_)
    , max_block_size(max_block_size_)
    , log(getLogger("PasteJoinAlgorithm"))
{
    if (input_headers.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PasteJoinAlgorithm requires exactly two inputs");

    auto strictness = table_join->getTableJoin().strictness();
    if (strictness != JoinStrictness::Any && strictness != JoinStrictness::All)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "PasteJoinAlgorithm is not implemented for strictness {}", strictness);

    auto kind = table_join->getTableJoin().kind();
    if (!isPaste(kind))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "PasteJoinAlgorithm is not implemented for kind {}", kind);
}

static void prepareChunk(Chunk & chunk)
{
    if (!chunk)
        return;

    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    chunk.setColumns(std::move(columns), num_rows);
}

IMergingAlgorithm::MergedStats PasteJoinAlgorithm::getMergedStats() const
{
    return
    {
        .bytes = stat.num_bytes[0] + stat.num_bytes[1],
        .rows = stat.num_rows[0] + stat.num_rows[1],
        .blocks = stat.num_blocks[0] + stat.num_blocks[1],
    };
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

    last_used_row[source_num] = 0;

    prepareChunk(input.chunk);
    chunks[source_num] = std::move(input.chunk);
}

IMergingAlgorithm::Status PasteJoinAlgorithm::merge()
{
    if (chunks[0].empty() || chunks[1].empty())
        return Status({}, true);
    if (last_used_row[0] >= chunks[0].getNumRows())
        return Status(0);
    if (last_used_row[1] >= chunks[1].getNumRows())
        return Status(1);

    /// We have unused rows from both inputs
    size_t result_num_rows = std::min(chunks[0].getNumRows() - last_used_row[0], chunks[1].getNumRows() - last_used_row[1]);

    Chunk result;
    for (size_t source_num = 0; source_num < 2; ++source_num)
        for (const auto & col : chunks[source_num].getColumns())
            result.addColumn(col->cut(last_used_row[source_num], result_num_rows));
    last_used_row[0] += result_num_rows;
    last_used_row[1] += result_num_rows;

    return Status(std::move(result));
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
    , log(getLogger("PasteJoinTransform"))
{
}

void PasteJoinTransform::onFinish() {};

}
