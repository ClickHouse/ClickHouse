#include <Columns/ColumnSparse.h>
#include <Core/Block.h>
#include <Processors/Transforms/BlockNestedLoopJoinData.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
    extern const Event JoinBuildTableRowCount;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

EmptyBuildSideAction emptyBuildSideActionFor(JoinKind kind, JoinStrictness strictness)
{
    /// With no build row at all, the only rows a join can still produce are unmatched probe rows,
    /// so the answer is exactly "does this kind keep an unmatched probe row". `SEMI` is the
    /// exception among the left-driven kinds: it keeps a probe row only when it does match.
    if (strictness == JoinStrictness::Semi)
        return EmptyBuildSideAction::ProduceNothing;
    return isLeftOrFull(kind) ? EmptyBuildSideAction::PassProbeRowsPadded : EmptyBuildSideAction::ProduceNothing;
}

bool needsBuildSideMatchFlags(JoinKind kind, JoinStrictness strictness)
{
    /// `ANY INNER` disables the cartesian product on both sides, so it needs the flags to give a
    /// build row to at most one probe row, even though it emits no build row of its own afterwards.
    if (strictness == JoinStrictness::Any && isInner(kind))
        return true;
    if (strictness == JoinStrictness::Semi || strictness == JoinStrictness::Anti)
        return isRight(kind);
    return isRightOrFull(kind);
}

bool keepsUnmatchedBuildRows(JoinKind kind, JoinStrictness strictness)
{
    /// `RIGHT SEMI` is the exception among the right-driven kinds: it keeps a build row only when
    /// it does match, which is the other half of the same flag scan.
    if (strictness == JoinStrictness::Semi)
        return false;
    if (strictness == JoinStrictness::Anti)
        return isRight(kind);
    return isRightOrFull(kind);
}

BlockNestedLoopJoinData::BlockNestedLoopJoinData(
    SharedHeader build_header_, JoinKind kind_, JoinStrictness strictness_, const SizeLimits & size_limits_)
    : build_header(std::move(build_header_))
    , kind(kind_)
    , strictness(strictness_)
    , size_limits(size_limits_)
    , empty_build_side_action(emptyBuildSideActionFor(kind_, strictness_))
    , needs_match_flags(needsBuildSideMatchFlags(kind_, strictness_))
{
}

bool BlockNestedLoopJoinData::addBlock(Block block, size_t num_rows)
{
    if (isFinished())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add a block to a finished block nested loop join build side");

    /// A block with no columns still carries rows, and the row count alone can decide the result
    /// (an empty build side is not the same as a build side of rows with nothing selected from it).
    chassert(block.columns() == 0 || block.rows() == num_rows);
    if (num_rows == 0)
        return true;

    assertCompatibleHeader(block, *build_header, "block nested loop join build side");

    /// The step outputs the concatenation of the input headers, whose columns are all full, and the
    /// probe builds its tiles by indexing into the stored columns. Const and Sparse cannot serve
    /// either purpose; Replicated can, and is kept as it is because unwrapping it would copy.
    Columns columns = block.getColumns();
    for (auto & column : columns)
        column = recursiveRemoveSparse(column->convertToFullColumnIfConst());

    StoredBlock stored_block(std::move(columns), ScatteredBlock::Selector(num_rows));
    size_t block_bytes = stored_block.allocatedBytes();

    size_t rows_in_join = 0;
    size_t bytes_in_join = 0;
    {
        std::lock_guard lock(mutex);
        stored_block.block_no = static_cast<UInt32>(blocks.size());
        blocks.push_back(std::move(stored_block));
        rows_in_join = total_rows.fetch_add(num_rows, std::memory_order_relaxed) + num_rows;
        bytes_in_join = total_bytes.fetch_add(block_bytes, std::memory_order_relaxed) + block_bytes;
    }

    ProfileEvents::increment(ProfileEvents::JoinBuildTableRowCount, num_rows);

    return size_limits.check(rows_in_join, bytes_in_join, "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
}

void BlockNestedLoopJoinData::setBuildSideTotals(Block totals)
{
    if (isFinished())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot set the totals of a finished block nested loop join build side");

    std::lock_guard lock(mutex);
    build_side_totals = std::move(totals);
}

const Block & BlockNestedLoopJoinData::getBuildSideTotals() const
{
    assertFinished("the build side totals");
    return TSA_SUPPRESS_WARNING_FOR_READ(build_side_totals);
}

void BlockNestedLoopJoinData::finish()
{
    std::lock_guard lock(mutex);

    if (finished.load(std::memory_order_relaxed))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Block nested loop join build side is finished twice");

    row_offsets.resize(blocks.size() + 1);
    size_t offset = 0;
    for (size_t i = 0; i < blocks.size(); ++i)
    {
        row_offsets[i] = offset;
        offset += blocks[i].selector.size();
    }
    row_offsets.back() = offset;
    chassert(offset == total_rows.load(std::memory_order_relaxed));

    if (needs_match_flags && offset != 0)
        matched_flags = std::make_unique<std::atomic_bool[]>(offset);

    finished.store(true, std::memory_order_release);
}

void BlockNestedLoopJoinData::setBuildRowMatched(size_t global_row)
{
    chassert(global_row < getTotalRows());
    matched_flags[global_row].store(true, std::memory_order_relaxed);
}

bool BlockNestedLoopJoinData::claimBuildRow(size_t global_row)
{
    chassert(global_row < getTotalRows());
    /// An atomic read-modify-write, so exactly one probe stream finds the flag unset and takes the
    /// row. Relaxed is enough: nothing but the claim itself travels through this access.
    return !matched_flags[global_row].exchange(true, std::memory_order_relaxed);
}

bool BlockNestedLoopJoinData::isBuildRowMatched(size_t global_row) const
{
    chassert(global_row < getTotalRows());
    return matched_flags[global_row].load(std::memory_order_relaxed);
}

void BlockNestedLoopJoinData::assertFinished(const char * what) const
{
    if (!isFinished())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Block nested loop join build side is not finished, cannot read {}", what);
}

const std::vector<StoredBlock> & BlockNestedLoopJoinData::getBlocks() const
{
    assertFinished("the stored blocks");
    return TSA_SUPPRESS_WARNING_FOR_READ(blocks);
}

const std::vector<size_t> & BlockNestedLoopJoinData::getRowOffsets() const
{
    assertFinished("the row offsets");
    return TSA_SUPPRESS_WARNING_FOR_READ(row_offsets);
}

bool BlockNestedLoopJoinData::isBuildSideEmpty() const
{
    assertFinished("the build side row count");
    return getTotalRows() == 0;
}

BlockNestedLoopBuildTransform::BlockNestedLoopBuildTransform(
    SharedHeader input_header, BlockNestedLoopJoinDataPtr data_, FinishCounterPtr finish_counter_)
    : IProcessor({std::move(input_header)}, {Block()})
    , data(std::move(data_))
    , finish_counter(std::move(finish_counter_))
{
}

InputPort * BlockNestedLoopBuildTransform::addTotalsPort()
{
    if (inputs.size() > 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Totals port was already added to BlockNestedLoopBuildTransform");

    return &inputs.emplace_back(inputs.front().getHeader(), this);
}

IProcessor::Status BlockNestedLoopBuildTransform::prepare()
{
    auto & output = outputs.front();
    auto & input = inputs.front();

    if (output.isFinished())
    {
        for (auto & in : inputs)
            in.close();
        finishBuild();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        for (auto & in : inputs)
            in.setNotNeeded();
        return Status::PortFull;
    }

    if (stop_reading)
        input.close();
    else if (!input.isFinished())
    {
        input.setNeeded();

        if (!input.hasData())
            return Status::NeedData;

        chunk = input.pull(true);
        return Status::Ready;
    }

    /// The totals row is stored after the build rows, so that the store is closed only once it is in.
    if (inputs.size() > 1)
    {
        auto & totals_input = inputs.back();
        if (!totals_input.isFinished())
        {
            totals_input.setNeeded();

            if (!totals_input.hasData())
                return Status::NeedData;

            chunk = totals_input.pull(true);
            for_totals = true;
            return Status::Ready;
        }
    }

    finishBuild();
    output.finish();
    return Status::Finished;
}

void BlockNestedLoopBuildTransform::work()
{
    auto num_rows = chunk.getNumRows();
    auto block = inputs.front().getHeader().cloneWithColumns(chunk.detachColumns());
    if (for_totals)
        data->setBuildSideTotals(std::move(block));
    else
        stop_reading = !data->addBlock(std::move(block), num_rows);
}

void BlockNestedLoopBuildTransform::finishBuild()
{
    /// Exactly one stream observes `isLast`, and it does so only once every stream has stopped
    /// appending, so the store is closed exactly once and never while a block is still going in.
    if (std::exchange(build_finished, true))
        return;

    if (finish_counter->isLast())
        data->finish();
}

}
