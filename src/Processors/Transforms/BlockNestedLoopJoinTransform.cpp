#include <Columns/ColumnConst.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/Defines.h>
#include <Processors/Transforms/BlockNestedLoopJoinTransform.h>
#include <Common/ProfileEvents.h>
#include <Common/assert_cast.h>

#include <limits>
#include <numeric>
#include <utility>

namespace ProfileEvents
{
    extern const Event JoinProbeTableRowCount;
    extern const Event JoinResultRowCount;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

using PairSelection = BlockNestedLoopProbeTransform::PairSelection;

/// Which pairs that satisfy the condition are part of the result: `ALL` keeps every one of them,
/// `ANY` and `SEMI` keep one per row of the side they are driven by, and an `ANTI` result is made
/// of the rows that matched nothing.
///
/// `join_any_take_last_row` is not honoured here, as the setting itself documents: it applies to
/// the `Join` table engine and the hash-based algorithms. With no join key there is no group of
/// rows to take the last of, and the store's block order is the order the build streams happened
/// to fill it in, so "the last matching row" would name nothing in particular while costing the
/// early exit that makes `ANY` worth choosing.
PairSelection pairSelectionFor(JoinKind kind, JoinStrictness strictness)
{
    /// An explicit cartesian join has no strictness of its own.
    if (isCrossOrComma(kind))
        return PairSelection::AllPairs;

    switch (strictness)
    {
        case JoinStrictness::All:
            return PairSelection::AllPairs;
        case JoinStrictness::Any:
            if (isInner(kind))
                return PairSelection::OnePerRowOfBothSides;
            [[fallthrough]];
        case JoinStrictness::Semi:
            return isRight(kind) ? PairSelection::FirstPerBuildRow : PairSelection::FirstPerProbeRow;
        /// The old `ANY` (`any_join_distinct_right_table_keys`) joins one build row to every probe
        /// row whatever the kind; `RIGHT` and `FULL` add the unmatched build rows on top of that.
        case JoinStrictness::RightAny:
            return PairSelection::FirstPerProbeRow;
        case JoinStrictness::Anti:
            return PairSelection::NoPairs;
        case JoinStrictness::Asof:
        case JoinStrictness::Unspecified:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Block nested loop join does not support {} {} JOIN",
                toString(strictness), toString(kind));
    }
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

/// What one row of the padded half of an outer join's output costs. A type's default value is not
/// always cheap - a `FixedString(N)` default is as wide as a matched one - and nothing but the
/// column itself says what it costs, so the cost is measured on a sample of defaults.
size_t estimateDefaultRowBytes(const Block & header, size_t begin, size_t end)
{
    /// Enough rows for the per-row cost to outweigh a column's fixed overhead.
    constexpr size_t SAMPLE_ROWS = 128;

    Columns columns;
    columns.reserve(end - begin);
    for (size_t i = begin; i < end; ++i)
    {
        auto column = header.getByPosition(i).type->createColumn();
        column->insertManyDefaults(SAMPLE_ROWS);
        columns.push_back(std::move(column));
    }
    return estimateRowBytes(columns, SAMPLE_ROWS);
}

/// How many rows one output chunk may hold under both limits, for rows of about `row_bytes` each.
size_t outputChunkRowLimit(size_t max_block_size, size_t max_block_bytes, size_t row_bytes)
{
    size_t limit = max_block_size != 0 ? max_block_size : std::numeric_limits<size_t>::max();
    if (max_block_bytes != 0 && row_bytes != 0)
        limit = std::min(limit, std::max<size_t>(1, max_block_bytes / row_bytes));
    return limit;
}

/// How many candidate pairs one tile holds. The bound is the operator's own, deliberately not
/// `max_joined_block_size_rows`: what it limits is not the output chunk but how large the columns the
/// condition is evaluated on grow, and it has to stay small however large the chunks the query asks
/// for - a wide intermediate value in the condition costs this many rows of it.
constexpr size_t TILE_PAIRS = DEFAULT_BLOCK_SIZE;

/// How many candidate pairs one `work` call may evaluate before returning without an output chunk.
/// A walk that matches nothing produces no output for a long time, and this is what gives the
/// executor a chance to notice cancellation; how long it may be held is not a property of how large
/// the chunks the query asks for are, so the budget does not follow `max_block_size`.
constexpr size_t WORK_BUDGET_PAIRS = 8 * TILE_PAIRS;

/// How many bytes of build blocks a probe stream may keep alive for pairs it has not emitted yet.
/// The bound is the operator's own, deliberately not `max_joined_block_size_bytes`: what it limits
/// is not the output chunk but the store's working set, and it has to stay small however large the
/// chunks the query asks for.
constexpr size_t MAX_RETAINED_BUILD_BYTES = 4 * 1024 * 1024;

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
    , build_reader(data)
    , predicate(std::move(predicate_))
    , max_block_size(max_block_size_)
    , max_block_bytes(max_block_bytes_)
    , pair_selection(pairSelectionFor(data->getKind(), data->getStrictness()))
    , keep_unmatched_probe_rows(keepsUnmatchedProbeRows(data->getKind(), data->getStrictness()))
    , flag_matched_build_rows(data->hasBuildSideMatchFlags())
    , claim_build_rows(
        pair_selection == PairSelection::FirstPerBuildRow || pair_selection == PairSelection::OnePerRowOfBothSides)
    /// A probe row leaves the walk as soon as it has its pair - unless the result still needs its
    /// other pairs, or unless the build rows it would match further on must be kept out of the
    /// scan for unmatched build rows.
    , early_exit_per_probe_row(
        pair_selection != PairSelection::AllPairs && pair_selection != PairSelection::FirstPerBuildRow
        && !keepsUnmatchedBuildRows(data->getKind(), data->getStrictness()))
    , track_probe_row_match(
        keep_unmatched_probe_rows || early_exit_per_probe_row
        || pair_selection == PairSelection::FirstPerProbeRow || pair_selection == PairSelection::OnePerRowOfBothSides)
{
    if (claim_build_rows && !flag_matched_build_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Block nested loop join keeps no build-side match flags to give each build row to one probe row for {} {} JOIN",
            toString(data->getStrictness()), toString(data->getKind()));

    if (output_header->columns() != probe_header->columns() + data->getHeader()->columns())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Block nested loop join output header [{}] is not the concatenation of its inputs [{}] and [{}]",
            output_header->dumpStructure(), probe_header->dumpStructure(), data->getHeader()->dumpStructure());

    if (keep_unmatched_probe_rows)
        padded_build_row_bytes = estimateDefaultRowBytes(*data->getHeader(), 0, data->getHeader()->columns());

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
        releaseProbeState();
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

    if (has_probe_chunk || pending_probe_chunk)
        return Status::Ready;

    if (input.isFinished())
    {
        output.finish();
        releaseProbeState();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    pending_probe_chunk = input.pull(true);
    return Status::Ready;
}

void BlockNestedLoopProbeTransform::startProbeChunk(Chunk chunk)
{
    probe_num_rows = chunk.getNumRows();
    ProfileEvents::increment(ProfileEvents::JoinProbeTableRowCount, probe_num_rows);
    probe_columns = chunk.detachColumns();
    /// The tile indexes into these columns and the output gathers from them; neither is possible
    /// on a Const or Sparse representation.
    for (auto & column : probe_columns)
        column = recursiveRemoveSparse(column->convertToFullColumnIfConst());

    has_probe_chunk = true;
    probe_window_cursor = 0;
    build_block_cursor = 0;
    build_row_cursor = 0;
    current_build_block.reset();
    unmatched_probe_cursor = 0;
    matched_probe_rows.clear();
    matched_build_rows.clear();
    matched_rows_offset = 0;
    build_runs.clear();
    retained_build_bytes = 0;
    probe_row_matched.clear();
    active_probe_rows.clear();

    /// A probe chunk with no rows has no pair to evaluate, and unmatched build rows - the only
    /// rows an empty probe side can still produce - are emitted by a stage of their own.
    if (probe_num_rows == 0)
    {
        stage = Stage::Done;
        return;
    }

    stage = Stage::Matching;
    probe_row_bytes = estimateRowBytes(probe_columns, probe_num_rows);

    if (track_probe_row_match)
        probe_row_matched.resize_fill(probe_num_rows, 0);

    active_probe_rows.resize_exact(probe_num_rows);
    std::iota(active_probe_rows.begin(), active_probe_rows.end(), UInt64(0));
}

void BlockNestedLoopProbeTransform::work()
{
    if (pending_probe_chunk)
    {
        auto chunk = std::move(*pending_probe_chunk);
        pending_probe_chunk.reset();
        startProbeChunk(std::move(chunk));
    }

    size_t evaluated_pairs = 0;

    while (!output_chunk)
    {
        switch (stage)
        {
            case Stage::Matching:
            {
                if (build_block_cursor < data->getNumBlocks() && !hasFullOutputChunk())
                {
                    evaluated_pairs += matchNextTile();
                    if (evaluated_pairs >= WORK_BUDGET_PAIRS)
                        return;
                    continue;
                }

                if (numPendingPairs() != 0)
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
                current_build_block.reset();
                return;
            }
        }
    }
}

/// TODO: prune tiles by interval arithmetic over the condition against the min/max of the tile's
/// values on each side, to skip a provably-empty tile and to shortcut a provably-all-true one without
/// evaluating the condition on it at all.
size_t BlockNestedLoopProbeTransform::matchNextTile()
{
    /// Every path that empties the walk also puts the build cursor past the last block, so a tile is
    /// never asked for once there is no probe row left to build one from.
    chassert(!active_probe_rows.empty());

    if (!current_build_block)
    {
        current_build_block = build_reader.read(build_block_cursor);
        /// The store builds every block over the whole of its columns, which is what makes a build
        /// row's position in the block the same as its index into the block's columns - the equality
        /// the match flags and the unmatched-build-rows scan are both indexed by.
        chassert(current_build_block->selector.isContinuousRange() && current_build_block->selector.getRange().first == 0);
        build_row_bytes = estimateRowBytes(current_build_block->columns, current_build_block->selector.size());
    }

    const auto & block = *current_build_block;
    const size_t block_rows = block.selector.size();
    /// The tile is sized in pairs, independently of the output limit: it bounds the columns the
    /// condition is evaluated on, while the output chunk is cut to `max_block_size` when the
    /// accumulated pairs are materialized. Both sides are windowed, so a probe chunk larger than the
    /// budget is swept in several passes over the same build rows.
    /// The build sub-range is sized from the widest window of the sweep rather than from the one at
    /// hand: a narrower last window must not reach further into the block than the windows before
    /// it, or the build rows in between would be left out of the walk.
    const size_t probe_window_rows = std::min<size_t>(active_probe_rows.size(), TILE_PAIRS);
    const size_t tile_probe_rows = std::min(active_probe_rows.size() - probe_window_cursor, probe_window_rows);
    const size_t build_rows_per_tile = std::max<size_t>(1, TILE_PAIRS / probe_window_rows);
    const size_t tile_build_rows = std::min(block_rows - build_row_cursor, build_rows_per_tile);
    const size_t tile_rows = tile_probe_rows * tile_build_rows;

    auto probe_tile_indexes = ColumnUInt64::create();
    auto build_tile_indexes = ColumnUInt64::create();
    {
        auto & probe_values = probe_tile_indexes->getData();
        auto & build_values = build_tile_indexes->getData();
        probe_values.resize_exact(tile_rows);
        build_values.resize_exact(tile_rows);

        /// The tile is probe-row major: pair `i * tile_build_rows + j` is the `i`-th probe row still
        /// in the walk against the `j`-th build row of the tile.
        for (size_t j = 0; j < tile_build_rows; ++j)
            build_values[j] = block.selector[build_row_cursor + j];
        for (size_t i = 0; i < tile_probe_rows; ++i)
        {
            const size_t offset = i * tile_build_rows;
            if (i != 0)
                memcpy(&build_values[offset], build_values.data(), tile_build_rows * sizeof(UInt64));
            for (size_t j = 0; j < tile_build_rows; ++j)
                probe_values[offset + j] = active_probe_rows[probe_window_cursor + i];
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
    /// The condition answers one candidate pair per row of the tile. The step constructor rejects
    /// the one function that can break that, so a mismatch here is a bug rather than a bad query.
    if (num_rows != tile_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Block nested loop join condition returned {} rows for a tile of {} candidate pairs", num_rows, tile_rows);

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
        appendMatchedPairs(*matched_probe, *matched_build, num_matched);

    /// The build rows are only left behind once every probe row still in the walk has been matched
    /// against them. That keeps the order a pair is seen in - and with it which pair a strictness
    /// that takes one per row settles on - the same as it would be for a single-window sweep.
    probe_window_cursor += tile_probe_rows;
    if (probe_window_cursor < active_probe_rows.size())
        return tile_rows;
    probe_window_cursor = 0;

    build_row_cursor += tile_build_rows;
    if (build_row_cursor >= block_rows)
    {
        build_row_cursor = 0;
        ++build_block_cursor;
        current_build_block.reset();
    }

    if (early_exit_per_probe_row)
    {
        dropMatchedProbeRows();
        /// Every probe row of the chunk has its match: no build row further on can change the result.
        if (active_probe_rows.empty())
        {
            build_block_cursor = data->getNumBlocks();
            build_row_cursor = 0;
            current_build_block.reset();
        }
    }

    return tile_rows;
}

void BlockNestedLoopProbeTransform::appendMatchedPairs(
    const IColumn & matched_probe, const IColumn & matched_build, size_t num_matched)
{
    const auto & probe_values = assert_cast<const ColumnUInt64 &>(matched_probe).getData();
    const auto & build_values = assert_cast<const ColumnUInt64 &>(matched_build).getData();
    /// A row's index into its block's columns is also its position in the block, so the global row
    /// number the flags are indexed by is just the block's row offset plus it.
    const size_t block_row_offset = flag_matched_build_rows ? data->getRowOffsets()[build_block_cursor] : 0;

    if (pair_selection == PairSelection::AllPairs)
    {
        matched_probe_rows.insert(probe_values.begin(), probe_values.end());
        matched_build_rows.insert(build_values.begin(), build_values.end());
        addBuildRun(num_matched);

        if (track_probe_row_match)
            for (auto probe_row : probe_values)
                probe_row_matched[probe_row] = 1;

        if (flag_matched_build_rows)
            for (auto build_row : build_values)
                data->setBuildRowMatched(block_row_offset + build_row);

        return;
    }

    /// The tile is probe-row major, so a probe row's first match comes before its others, and a
    /// build row goes to the earliest probe row of the tile that matched it.
    size_t num_selected = 0;
    for (size_t i = 0; i < num_matched; ++i)
    {
        const UInt64 probe_row = probe_values[i];
        const UInt64 build_row = build_values[i];

        bool selected = false;
        switch (pair_selection)
        {
            case PairSelection::FirstPerProbeRow:
                selected = !probe_row_matched[probe_row];
                break;
            case PairSelection::FirstPerBuildRow:
                selected = data->claimBuildRow(block_row_offset + build_row);
                break;
            case PairSelection::OnePerRowOfBothSides:
                /// The build row is taken only when it settles the probe row, so a probe row whose
                /// match is already spoken for keeps looking.
                selected = !probe_row_matched[probe_row] && data->claimBuildRow(block_row_offset + build_row);
                break;
            case PairSelection::NoPairs:
            case PairSelection::AllPairs:
                break;
        }

        /// A probe row of an `ANY INNER` counts as matched only once it has its pair; under every
        /// other selection a match is final, and the build rows it passed over stay behind.
        if (track_probe_row_match && (selected || pair_selection != PairSelection::OnePerRowOfBothSides))
            probe_row_matched[probe_row] = 1;
        /// A claim flags the build row itself; the other selections flag the rows they matched but
        /// did not take, so that the stage after the probe does not report them as unmatched.
        if (flag_matched_build_rows && !claim_build_rows)
            data->setBuildRowMatched(block_row_offset + build_row);

        if (selected)
        {
            matched_probe_rows.push_back(probe_row);
            matched_build_rows.push_back(build_row);
            ++num_selected;
        }
    }

    if (num_selected != 0)
        addBuildRun(num_selected);
}

void BlockNestedLoopProbeTransform::addBuildRun(size_t length)
{
    if (!build_runs.empty() && build_runs.back().block_index == build_block_cursor)
    {
        build_runs.back().length += length;
        return;
    }

    /// A block the store hands out as it is costs the run nothing; one it had to decompress or read
    /// back from disk lives only as long as the run holds it.
    const size_t retained_bytes
        = data->isBlockSharedInMemory(build_block_cursor) ? 0 : current_build_block->allocatedBytes();
    retained_build_bytes += retained_bytes;
    build_runs.push_back({build_block_cursor, length, current_build_block, retained_bytes});
}

void BlockNestedLoopProbeTransform::dropMatchedProbeRows()
{
    size_t kept = 0;
    for (auto probe_row : active_probe_rows)
        if (!probe_row_matched[probe_row])
            active_probe_rows[kept++] = probe_row;
    active_probe_rows.resize(kept);
}

bool BlockNestedLoopProbeTransform::hasFullOutputChunk() const
{
    if (max_block_size != 0 && numPendingPairs() >= max_block_size)
        return true;
    if (max_block_bytes != 0 && numPendingPairs() * (probe_row_bytes + build_row_bytes) >= max_block_bytes)
        return true;
    /// The pending pairs keep alive every build block they came from. A condition selective enough to
    /// match a few rows in each of them would otherwise pin the whole build side, decompressed and
    /// read back from disk, which is what compressing and spilling it exist to avoid. Cutting the
    /// chunk here releases the blocks its pairs were gathered from.
    return retained_build_bytes >= MAX_RETAINED_BUILD_BYTES;
}

size_t BlockNestedLoopProbeTransform::maxOutputChunkRows(size_t row_bytes) const
{
    /// One tile's worth of pairs is accumulated before the size of the pending output is looked at
    /// again, so the byte limit has to cut the chunk here as well, not only stop the accumulation.
    return outputChunkRowLimit(max_block_size, max_block_bytes, row_bytes);
}

Chunk BlockNestedLoopProbeTransform::takeMatchedRows()
{
    const size_t total_rows = numPendingPairs();
    const size_t num_rows = std::min(total_rows, maxOutputChunkRows(probe_row_bytes + build_row_bytes));
    chassert(num_rows != 0);

    const auto pending_probe_begin = matched_probe_rows.begin() + matched_rows_offset;
    const auto pending_build_begin = matched_build_rows.begin() + matched_rows_offset;

    Columns result;
    result.reserve(output_header->columns());

    {
        auto indexes = ColumnUInt64::create();
        indexes->getData().insert(pending_probe_begin, pending_probe_begin + num_rows);
        for (const auto & column : probe_columns)
            result.push_back(column->index(*indexes, 0));
    }

    /// The pairs of one output chunk can span several stored blocks; each group is gathered from
    /// its own block and the pieces are concatenated. A single group is the common case and needs
    /// no concatenation. The gather indexes of a group are the same for every column of its block,
    /// so they are built once here rather than per column.
    std::vector<BuildRun> emitted_runs;
    std::vector<ColumnPtr> emitted_run_indexes;
    for (size_t rest = num_rows, offset = 0; rest != 0;)
    {
        const auto & run = build_runs.at(emitted_runs.size());
        const size_t length = std::min(rest, run.length);

        auto indexes = ColumnUInt64::create();
        indexes->getData().insert(pending_build_begin + offset, pending_build_begin + offset + length);
        emitted_run_indexes.push_back(std::move(indexes));

        emitted_runs.push_back({run.block_index, length, run.block});
        offset += length;
        rest -= length;
    }

    const size_t num_build_columns = data->getHeader()->columns();
    for (size_t i = 0; i < num_build_columns; ++i)
    {
        MutableColumnPtr target;
        for (size_t run_index = 0; run_index < emitted_runs.size(); ++run_index)
        {
            const auto & run = emitted_runs[run_index];
            auto part = run.block->columns[i]->index(*emitted_run_indexes[run_index], 0);
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

    matched_rows_offset += num_rows;
    dropEmittedPairs();
    for (const auto & run : emitted_runs)
    {
        if (build_runs.front().length == run.length)
        {
            retained_build_bytes -= build_runs.front().retained_bytes;
            build_runs.pop_front();
        }
        else
            build_runs.front().length -= run.length;
    }

    ProfileEvents::increment(ProfileEvents::JoinResultRowCount, num_rows);

    Chunk chunk;
    chunk.setColumns(std::move(result), num_rows);
    return chunk;
}

void BlockNestedLoopProbeTransform::dropEmittedPairs()
{
    /// Moving the pairs still pending costs what they are, so the prefix is only dropped once it is
    /// at least as large as they are: over the drain of one tile that amounts to moving each pair a
    /// constant number of times, rather than once per output chunk cut from the tile.
    if (matched_rows_offset < numPendingPairs())
        return;

    if (matched_rows_offset == matched_probe_rows.size())
    {
        matched_probe_rows.clear();
        matched_build_rows.clear();
    }
    else
    {
        matched_probe_rows.erase(matched_probe_rows.begin(), matched_probe_rows.begin() + matched_rows_offset);
        matched_build_rows.erase(matched_build_rows.begin(), matched_build_rows.begin() + matched_rows_offset);
    }

    matched_rows_offset = 0;
}

Chunk BlockNestedLoopProbeTransform::takeUnmatchedProbeRows()
{
    auto indexes = ColumnUInt64::create();
    auto & unmatched = indexes->getData();
    /// The build side of these rows is padded with the type defaults, whose cost is the same for
    /// every row of the chunk but is not always negligible.
    const size_t limit = maxOutputChunkRows(probe_row_bytes + padded_build_row_bytes);
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

    ProfileEvents::increment(ProfileEvents::JoinResultRowCount, unmatched.size());

    Chunk chunk;
    chunk.setColumns(std::move(result), unmatched.size());
    return chunk;
}

void BlockNestedLoopProbeTransform::releaseProbeState()
{
    /// A finished processor is only destroyed with the pipeline, and the stage that emits the
    /// unmatched build rows runs after this one: the pairs accumulated for a chunk that will not be
    /// emitted, and above all the stored blocks they pinned, must not stay charged to the query
    /// until then.
    build_reader.release();
    build_runs.clear();
    retained_build_bytes = 0;
    current_build_block.reset();
    probe_columns.clear();
    matched_probe_rows = {};
    matched_build_rows = {};
    matched_rows_offset = 0;
    active_probe_rows = {};
    probe_row_matched = {};
    pending_probe_chunk.reset();
    output_chunk.reset();
    has_probe_chunk = false;
    stage = Stage::Done;
}

/// How many of the output columns come from the probe side, which is what this stage pads.
static size_t probeColumnCount(const Block & output_header, const Block & build_header)
{
    if (output_header.columns() < build_header.columns())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Block nested loop join output header [{}] does not end with its build side [{}]",
            output_header.dumpStructure(), build_header.dumpStructure());
    return output_header.columns() - build_header.columns();
}

BlockNestedLoopUnmatchedBuildRowsTransform::BlockNestedLoopUnmatchedBuildRowsTransform(
    SharedHeader output_header_,
    BlockNestedLoopJoinDataPtr data_,
    size_t max_block_size_,
    size_t max_block_bytes_,
    size_t stream_index_,
    size_t num_streams_)
    /// The rows this stage emits were read by the build side and are only handed out again here,
    /// so counting them as read would double them in the query's read progress and quotas.
    : ISource(std::move(output_header_), /*enable_auto_progress=*/ false)
    , data(std::move(data_))
    , build_reader(data)
    , num_probe_columns(probeColumnCount(getPort().getHeader(), *data->getHeader()))
    , padded_probe_row_bytes(estimateDefaultRowBytes(getPort().getHeader(), 0, num_probe_columns))
    , max_block_size(max_block_size_)
    , max_block_bytes(max_block_bytes_)
    , stream_index(stream_index_)
    , num_streams(num_streams_)
    , block_cursor(stream_index_)
{
    if (!data->hasBuildSideMatchFlags())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Block nested loop join keeps no build-side match flags for {} {} JOIN",
            toString(data->getStrictness()), toString(data->getKind()));
}

IProcessor::Status BlockNestedLoopUnmatchedBuildRowsTransform::prepare()
{
    auto status = ISource::prepare();
    /// The scan does not always reach the last block - a `LIMIT` above the join finishes the output
    /// port first - and a finished processor is only destroyed with the pipeline. The block the
    /// reader materialized and its handle on the temporary file must not stay charged to the query
    /// until then, so they go here rather than only where the scan runs out of blocks.
    if (status == Status::Finished)
        build_reader.release();
    return status;
}

Chunk BlockNestedLoopUnmatchedBuildRowsTransform::generate()
{
    const auto & header = getPort().getHeader();
    const auto & row_offsets = data->getRowOffsets();

    /// A spilled block is only reachable by a forward pass over the temporary file, so a strided
    /// scan would make every stream decode the whole of it. One stream walks them all instead.
    if (!std::exchange(scan_partitioned, true) && data->getNumSpilledBlocks() != 0)
    {
        if (stream_index != 0)
            return {};
        num_streams = 1;
    }

    /// One chunk comes from one stored block, so that its build columns are gathered in one go.
    /// A block in which every row matched contributes nothing and the scan moves on to the next.
    while (block_cursor < data->getNumBlocks())
    {
        const size_t block_index = block_cursor;
        const size_t block_rows = data->getBlockNumRows(block_index);
        const size_t block_row_offset = row_offsets[block_index];

        /// The flags alone say whether the rest of the block contributes anything, so one in which
        /// every row matched is never read back - and the block has to be at hand before the byte
        /// limit can be turned into a row count.
        size_t first_unmatched = row_cursor;
        while (first_unmatched < block_rows && data->isBuildRowMatched(block_row_offset + first_unmatched))
            ++first_unmatched;

        if (first_unmatched >= block_rows)
        {
            row_cursor = 0;
            block_cursor += num_streams;
            continue;
        }

        auto block = build_reader.read(block_index);
        const size_t limit = outputChunkRowLimit(
            max_block_size, max_block_bytes,
            padded_probe_row_bytes + estimateRowBytes(block->columns, block_rows));

        auto indexes = ColumnUInt64::create();
        auto & unmatched = indexes->getData();
        for (row_cursor = first_unmatched; row_cursor < block_rows && unmatched.size() < limit; ++row_cursor)
        {
            if (!data->isBuildRowMatched(block_row_offset + row_cursor))
                unmatched.push_back(row_cursor);
        }

        if (row_cursor >= block_rows)
        {
            row_cursor = 0;
            block_cursor += num_streams;
        }

        Columns result;
        result.reserve(header.columns());
        /// The default of the column type, which is NULL where `addToNullableIfNeeded` made the
        /// padded side's columns `Nullable` in the pre-join actions.
        for (size_t i = 0; i < num_probe_columns; ++i)
        {
            auto padded = header.getByPosition(i).type->createColumn();
            padded->insertManyDefaults(unmatched.size());
            result.push_back(std::move(padded));
        }
        for (const auto & column : block->columns)
            result.push_back(column->index(*indexes, 0));

        ProfileEvents::increment(ProfileEvents::JoinResultRowCount, unmatched.size());

        Chunk chunk;
        chunk.setColumns(std::move(result), unmatched.size());
        return chunk;
    }

    return {};
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
