#include <Columns/ColumnConst.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/Defines.h>
#include <Processors/Transforms/BlockNestedLoopJoinTransform.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

/// The kinds and strictnesses the probe handles on its own. `RIGHT`/`FULL` additionally need the
/// build-side used flags, and the early-exit strictnesses need their own walk over the build side.
bool isImplementedByProbe(JoinKind kind, JoinStrictness strictness)
{
    return strictness == JoinStrictness::All && (isInner(kind) || isLeft(kind) || isCrossOrComma(kind));
}

/// Whether a probe row that matched no build row is still part of the result, padded with the
/// build side's defaults.
bool keepsUnmatchedProbeRows(JoinKind kind, JoinStrictness strictness)
{
    if (strictness == JoinStrictness::Semi)
        return false;
    if (strictness == JoinStrictness::Anti)
        return isLeft(kind);
    return isLeftOrFull(kind);
}

/// The tile's view of `column`: `indexes` picks one source row per candidate pair. Lazy
/// replication keeps the source values from being copied into the tile; for narrow fixed-size
/// values the copy costs less than the indirection, which is what `isLazyReplicationUseful` decides.
ColumnPtr tileColumn(const ColumnPtr & column, const ColumnPtr & indexes)
{
    if (isLazyReplicationUseful(column))
        return ColumnReplicated::create(column, indexes);
    return column->index(*indexes, 0);
}

size_t estimateRowBytes(const Columns & columns, size_t num_rows)
{
    if (num_rows == 0)
        return 0;

    size_t bytes = 0;
    for (const auto & column : columns)
        bytes += column->byteSize();
    return (bytes + num_rows - 1) / num_rows;
}

}

BlockNestedLoopProbeTransform::BlockNestedLoopProbeTransform(
    SharedHeader probe_header_,
    SharedHeader output_header_,
    BlockNestedLoopJoinDataPtr data_,
    BlockNestedLoopPredicate predicate_,
    size_t max_block_size_,
    size_t max_block_bytes_)
    : IProcessor({probe_header_}, {output_header_})
    , probe_header(std::move(probe_header_))
    , output_header(std::move(output_header_))
    , data(std::move(data_))
    , predicate(std::move(predicate_))
    , max_block_size(max_block_size_)
    , max_block_bytes(max_block_bytes_)
    , implemented(isImplementedByProbe(data->getKind(), data->getStrictness()))
    , keep_unmatched_probe_rows(keepsUnmatchedProbeRows(data->getKind(), data->getStrictness()))
{
    if (output_header->columns() != probe_header->columns() + data->getHeader()->columns())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Block nested loop join output header [{}] is not the concatenation of its inputs [{}] and [{}]",
            output_header->dumpStructure(), probe_header->dumpStructure(), data->getHeader()->dumpStructure());

    for (const auto & required_column : predicate.actions->getRequiredColumnsWithTypes())
        predicate_input_header.insert(ColumnWithTypeAndName(nullptr, required_column.type, required_column.name));
    predicate_input_positions = predicate.actions->getInputPositions(predicate_input_header);
}

IProcessor::Status BlockNestedLoopProbeTransform::prepare()
{
    auto & output = outputs.front();
    auto & input = inputs.front();

    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    if (output_chunk)
    {
        output.push(std::move(*output_chunk));
        output_chunk.reset();
        return Status::PortFull;
    }

    if (has_probe_chunk)
        return Status::Ready;

    if (input.isFinished())
    {
        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    startProbeChunk(input.pull(true));
    return Status::Ready;
}

void BlockNestedLoopProbeTransform::startProbeChunk(Chunk chunk)
{
    probe_num_rows = chunk.getNumRows();
    probe_columns = chunk.detachColumns();
    /// The tile indexes into these columns and the output gathers from them; neither is possible
    /// on a Const or Sparse representation.
    for (auto & column : probe_columns)
        column = recursiveRemoveSparse(column->convertToFullColumnIfConst());

    has_probe_chunk = true;
    build_block_cursor = 0;
    build_row_cursor = 0;
    unmatched_probe_cursor = 0;
    matched_probe_rows.clear();
    matched_build_rows.clear();
    build_runs.clear();
    probe_row_matched.clear();

    /// A probe chunk with no rows has no pair to evaluate, and unmatched build rows - the only
    /// rows an empty probe side can still produce - are emitted by a stage of their own.
    if (probe_num_rows == 0)
    {
        stage = Stage::Done;
        return;
    }

    stage = Stage::Matching;
    /// The tile is sized in pairs, independently of the output limit: it bounds the intermediate
    /// columns the condition is evaluated on, while the output chunk is cut to `max_block_size`
    /// when the accumulated pairs are materialized.
    build_rows_per_tile = std::max<size_t>(1, DEFAULT_BLOCK_SIZE / probe_num_rows);
    probe_row_bytes = estimateRowBytes(probe_columns, probe_num_rows);

    if (keep_unmatched_probe_rows)
        probe_row_matched.resize_fill(probe_num_rows, 0);
}

void BlockNestedLoopProbeTransform::work()
{
    if (!implemented)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Block nested loop join does not support {} {} JOIN yet",
            toString(data->getStrictness()), toString(data->getKind()));

    /// A walk that matches nothing produces no output for a long time, so it is cut into pieces
    /// that give the executor a chance to notice cancellation.
    const size_t work_budget = 8 * std::max<size_t>(DEFAULT_BLOCK_SIZE, max_block_size);
    size_t evaluated_pairs = 0;

    while (!output_chunk)
    {
        switch (stage)
        {
            case Stage::Matching:
            {
                if (build_block_cursor < data->getBlocks().size() && !hasFullOutputChunk())
                {
                    evaluated_pairs += matchNextTile();
                    if (evaluated_pairs >= work_budget)
                        return;
                    continue;
                }

                if (!matched_probe_rows.empty())
                {
                    output_chunk = takeMatchedRows();
                    continue;
                }

                stage = keep_unmatched_probe_rows ? Stage::UnmatchedProbeRows : Stage::Done;
                continue;
            }
            case Stage::UnmatchedProbeRows:
            {
                if (unmatched_probe_cursor < probe_num_rows)
                {
                    /// A window in which every probe row matched yields nothing; the cursor still
                    /// advances, so the walk ends.
                    auto chunk = takeUnmatchedProbeRows();
                    if (chunk.getNumRows() != 0)
                        output_chunk = std::move(chunk);
                    continue;
                }

                stage = Stage::Done;
                continue;
            }
            case Stage::Done:
            {
                has_probe_chunk = false;
                probe_columns.clear();
                return;
            }
        }
    }
}

size_t BlockNestedLoopProbeTransform::matchNextTile()
{
    const auto & block = data->getBlocks()[build_block_cursor];
    const size_t block_rows = block.selector.size();
    const size_t tile_build_rows = std::min(block_rows - build_row_cursor, build_rows_per_tile);
    const size_t tile_rows = probe_num_rows * tile_build_rows;

    if (build_row_cursor == 0)
        build_row_bytes = estimateRowBytes(block.columns, block_rows);

    auto probe_tile_indexes = ColumnUInt64::create();
    auto build_tile_indexes = ColumnUInt64::create();
    {
        auto & probe_values = probe_tile_indexes->getData();
        auto & build_values = build_tile_indexes->getData();
        probe_values.resize_exact(tile_rows);
        build_values.resize_exact(tile_rows);

        /// The tile is probe-row major: pair `i * tile_build_rows + j` is the probe row `i` against
        /// the `j`-th build row of the tile.
        for (size_t j = 0; j < tile_build_rows; ++j)
            build_values[j] = block.selector[build_row_cursor + j];
        for (size_t i = 0; i < probe_num_rows; ++i)
        {
            const size_t offset = i * tile_build_rows;
            if (i != 0)
                memcpy(&build_values[offset], build_values.data(), tile_build_rows * sizeof(UInt64));
            for (size_t j = 0; j < tile_build_rows; ++j)
                probe_values[offset + j] = i;
        }
    }

    const ColumnPtr probe_tile = std::move(probe_tile_indexes);
    const ColumnPtr build_tile = std::move(build_tile_indexes);

    Columns condition_inputs;
    condition_inputs.reserve(predicate.inputs.size());
    for (const auto & source : predicate.inputs)
    {
        const auto & column = source.side == 0 ? probe_columns[source.position] : block.columns[source.position];
        condition_inputs.push_back(tileColumn(column, source.side == 0 ? probe_tile : build_tile));
    }

    size_t num_rows = tile_rows;
    Columns condition = predicate.actions->executeOnColumns(
        std::move(condition_inputs), predicate_input_header, predicate_input_positions, num_rows);

    size_t num_matched = 0;
    ColumnPtr matched_probe;
    ColumnPtr matched_build;

    ConstantFilterDescription constant_condition(*condition.at(0));
    if (constant_condition.always_true)
    {
        num_matched = tile_rows;
        matched_probe = probe_tile;
        matched_build = build_tile;
    }
    else if (!constant_condition.always_false)
    {
        /// A NULL condition value is not a match, which is what `FilterDescription` makes of the
        /// null map of a `Nullable` condition column.
        FilterDescription matched(*condition.at(0));
        num_matched = matched.countBytesInFilter();
        if (num_matched != 0)
        {
            matched_probe = matched.filter(*probe_tile, num_matched);
            matched_build = matched.filter(*build_tile, num_matched);
        }
    }

    if (num_matched != 0)
    {
        const auto & probe_values = assert_cast<const ColumnUInt64 &>(*matched_probe).getData();
        const auto & build_values = assert_cast<const ColumnUInt64 &>(*matched_build).getData();
        matched_probe_rows.insert(probe_values.begin(), probe_values.end());
        matched_build_rows.insert(build_values.begin(), build_values.end());

        if (!build_runs.empty() && build_runs.back().block_index == build_block_cursor)
            build_runs.back().length += num_matched;
        else
            build_runs.push_back({build_block_cursor, num_matched});

        if (keep_unmatched_probe_rows)
            for (auto probe_row : probe_values)
                probe_row_matched[probe_row] = 1;
    }

    build_row_cursor += tile_build_rows;
    if (build_row_cursor >= block_rows)
    {
        build_row_cursor = 0;
        ++build_block_cursor;
    }

    return tile_rows;
}

bool BlockNestedLoopProbeTransform::hasFullOutputChunk() const
{
    if (max_block_size != 0 && matched_probe_rows.size() >= max_block_size)
        return true;
    return max_block_bytes != 0 && matched_probe_rows.size() * (probe_row_bytes + build_row_bytes) >= max_block_bytes;
}

Chunk BlockNestedLoopProbeTransform::takeMatchedRows()
{
    const size_t total_rows = matched_probe_rows.size();
    const size_t num_rows = max_block_size != 0 ? std::min(total_rows, max_block_size) : total_rows;
    chassert(num_rows != 0);

    Columns result;
    result.reserve(output_header->columns());

    {
        auto indexes = ColumnUInt64::create();
        indexes->getData().insert(matched_probe_rows.begin(), matched_probe_rows.begin() + num_rows);
        for (const auto & column : probe_columns)
            result.push_back(column->index(*indexes, 0));
    }

    /// The pairs of one output chunk can span several stored blocks; each group is gathered from
    /// its own block and the pieces are concatenated. A single group is the common case and needs
    /// no concatenation.
    std::vector<BuildRun> emitted_runs;
    for (size_t rest = num_rows; rest != 0;)
    {
        const auto & run = build_runs.at(emitted_runs.size());
        const size_t length = std::min(rest, run.length);
        emitted_runs.push_back({run.block_index, length});
        rest -= length;
    }

    const auto & blocks = data->getBlocks();
    const size_t num_build_columns = data->getHeader()->columns();
    for (size_t i = 0; i < num_build_columns; ++i)
    {
        MutableColumnPtr target;
        size_t offset = 0;
        for (const auto & run : emitted_runs)
        {
            auto indexes = ColumnUInt64::create();
            indexes->getData().insert(
                matched_build_rows.begin() + offset, matched_build_rows.begin() + offset + run.length);
            offset += run.length;

            auto part = blocks[run.block_index].columns[i]->index(*indexes, 0);
            if (emitted_runs.size() == 1)
            {
                result.push_back(std::move(part));
                break;
            }

            part = part->convertToFullColumnIfReplicated();
            if (!target)
            {
                target = data->getHeader()->getByPosition(i).type->createColumn();
                target->reserve(num_rows);
            }
            target->insertRangeFrom(*part, 0, part->size());
        }

        if (target)
            result.push_back(std::move(target));
    }

    matched_probe_rows.erase(matched_probe_rows.begin(), matched_probe_rows.begin() + num_rows);
    matched_build_rows.erase(matched_build_rows.begin(), matched_build_rows.begin() + num_rows);
    for (const auto & run : emitted_runs)
    {
        if (build_runs.front().length == run.length)
            build_runs.pop_front();
        else
            build_runs.front().length -= run.length;
    }

    Chunk chunk;
    chunk.setColumns(std::move(result), num_rows);
    return chunk;
}

Chunk BlockNestedLoopProbeTransform::takeUnmatchedProbeRows()
{
    auto indexes = ColumnUInt64::create();
    auto & unmatched = indexes->getData();
    const size_t limit = max_block_size != 0 ? max_block_size : probe_num_rows;
    while (unmatched_probe_cursor < probe_num_rows && unmatched.size() < limit)
    {
        if (!probe_row_matched[unmatched_probe_cursor])
            unmatched.push_back(unmatched_probe_cursor);
        ++unmatched_probe_cursor;
    }

    if (unmatched.empty())
        return {};

    Columns result;
    result.reserve(output_header->columns());
    for (const auto & column : probe_columns)
        result.push_back(column->index(*indexes, 0));

    /// The default of the column type, which is NULL where `addToNullableIfNeeded` made the padded
    /// side's columns `Nullable` in the pre-join actions.
    for (const auto & column_with_type : *data->getHeader())
    {
        auto padded = column_with_type.type->createColumn();
        padded->insertManyDefaults(unmatched.size());
        result.push_back(std::move(padded));
    }

    Chunk chunk;
    chunk.setColumns(std::move(result), unmatched.size());
    return chunk;
}

BlockNestedLoopTotalsTransform::BlockNestedLoopTotalsTransform(
    SharedHeader probe_header_,
    SharedHeader output_header_,
    BlockNestedLoopJoinDataPtr data_,
    bool probe_totals_are_default_)
    : ISimpleTransform(std::move(probe_header_), std::move(output_header_), /*skip_empty_chunks_=*/ true)
    , data(std::move(data_))
    , probe_totals_are_default(probe_totals_are_default_)
{
}

/// The totals value of the column at `position` of `source`, or the type's default when `source`
/// does not reach that far. `arrayJoin` in the totals expression can make the row count differ
/// from one, so the result is always cut back to a single row.
static ColumnPtr totalsColumnAt(const Block & source, size_t position, const DataTypePtr & type)
{
    if (position >= source.columns())
        return type->createColumnConstWithDefaultValue(1)->convertToFullColumnIfConst();

    auto column = source.getByPosition(position).column->convertToFullColumnIfConst();
    if (column->size() != 1)
        column = column->cloneResized(1);
    return column;
}

void BlockNestedLoopTotalsTransform::transform(Chunk & chunk)
{
    const auto & build_totals = data->getBuildSideTotals();
    if (probe_totals_are_default && build_totals.columns() == 0)
    {
        chunk.clear();
        return;
    }

    const auto & probe_header = getInputPort().getHeader();
    const auto & output_header = getOutputPort().getHeader();
    Block probe_totals = probe_header.cloneWithColumns(chunk.detachColumns());

    Columns columns;
    columns.reserve(output_header.columns());
    for (size_t i = 0; i < output_header.columns(); ++i)
    {
        const auto & target = output_header.getByPosition(i);
        columns.push_back(i < probe_header.columns()
            ? totalsColumnAt(probe_totals, i, target.type)
            : totalsColumnAt(build_totals, i - probe_header.columns(), target.type));
    }

    chunk.setColumns(std::move(columns), 1);
}

}
