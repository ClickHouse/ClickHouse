#include <Interpreters/ConstantJoin.h>

#include <algorithm>
#include <limits>
#include <vector>

#include <base/arithmeticOverflow.h>

#include <Columns/ColumnCompressed.h>
#include <Columns/ColumnReplicated.h>
#include <Common/Exception.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Core/Joins.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/TableJoin.h>
#include <base/defines.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

namespace
{

/// An explicit cartesian join or a join without any `ON`/`USING` clause matches unconditionally.
/// The old analyzer encodes constant-false `JOIN ON` as a keyless join that still has its `ON` expression,
/// so everything else defaults to false. The analyzer passes the value explicitly.
bool computeConstantPredicateValue(const TableJoin & table_join)
{
    if (auto join_expression_value = table_join.getJoinExpressionValue())
        return *join_expression_value;

    bool is_no_clause_join = table_join.getClauses().empty() && !table_join.hasOn() && !table_join.hasUsing();
    return isCrossOrComma(table_join.kind()) || is_no_clause_join;
}

/// The captured selected right row must not stay replicated: a one-row replicated copy would pin the source
/// block's nested columns, and `ConstantJoinSelectedRowResult` repeats the row into full output columns
/// with one bulk `insertManyFrom` per column.
Columns materializeSelectedRowColumns(Columns columns)
{
    for (auto & column : columns)
        column = column->convertToFullColumnIfReplicated();
    return columns;
}

}

ConstantJoin::ConstantJoin(std::shared_ptr<TableJoin> table_join_, SharedHeader right_sample_block_, bool any_take_last_row_)
    : table_join(std::move(table_join_))
    , tmp_data(table_join->getTempDataOnDisk())
    , constant_predicate_value(computeConstantPredicateValue(*table_join))
    , plan(makeOutputPlan(table_join->kind(), table_join->strictness(), constant_predicate_value, any_take_last_row_))
    , any_take_last_row(any_take_last_row_)
    , max_joined_block_rows(table_join->maxJoinedBlockRows())
    , max_joined_block_bytes(table_join->maxJoinedBlockBytes())
    , log(getLogger("ConstantJoin"))
{
    if (!isCrossOrComma(table_join->kind()) && !table_join->isJoinWithConstant() && !table_join->getClauses().empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ConstantJoin expects CROSS, comma or JOIN ON constant, got {}", table_join->kind());

    Block right_header = *right_sample_block_;
    JoinCommon::createMissedColumns(right_header);

    right_sample_block = materializeBlock(right_header);

    LOG_TEST(log, "Right header: {}", right_header.dumpStructure());
}

/// The whole join strategy is a pure function of facts known at construction: the join kind, the strictness,
/// and the constant predicate value. See the field comments in `OutputPlan` for what each decision controls.
ConstantJoin::OutputPlan ConstantJoin::makeOutputPlan(JoinKind kind, JoinStrictness strictness, bool constant_predicate_value, bool any_take_last_row)
{
    using LeftRowsToJoin = OutputPlan::LeftRowsToJoin;
    using RightRowsToJoin = OutputPlan::RightRowsToJoin;

    OutputPlan plan{
        .left_rows_to_join = LeftRowsToJoin::None,
        .right_rows_to_join = RightRowsToJoin::AllStoredRows,
        .store_right_rows = true,
        .emit_unmatched_left_rows = false,
        .emit_unmatched_right_rows = false,
        .select_last_right_row = false,
    };

    /// Intentionally mirrors `HashJoin`: `join_any_take_last_row` affects only the modes that join one selected
    /// right row, and never the `RIGHT` kinds, which join all right rows.
    plan.select_last_right_row = any_take_last_row && strictness == JoinStrictness::Any && !isRight(kind);

    /// An explicit cartesian join ignores strictness and has no unmatched rows to pad.
    if (isCrossOrComma(kind))
    {
        if (constant_predicate_value)
            plan.left_rows_to_join = LeftRowsToJoin::All;
    }
    else
    {
        /// ANTI/SEMI join can only be left or right
        chassert(isLeftOrRight(kind) || (strictness != JoinStrictness::Anti && strictness != JoinStrictness::Semi));
        const bool is_semi = strictness == JoinStrictness::Semi;

        plan.emit_unmatched_left_rows = isLeftOrFull(kind) && !is_semi;
        plan.emit_unmatched_right_rows = isRightOrFull(kind) && !is_semi;
    }

    /// With a false predicate no left row ever matches, so only the unmatched behaviors above remain.
    if (constant_predicate_value && !isCrossOrComma(kind))
    {
        switch (strictness)
        {
            case JoinStrictness::All:
                /// Every left row joins every right row.
                plan.left_rows_to_join = LeftRowsToJoin::All;
                plan.right_rows_to_join = RightRowsToJoin::AllStoredRows;
                break;
            case JoinStrictness::Any:
                if (isRight(kind))
                {
                    /// `RIGHT ANY` keeps the right side intact: the first left row joins all right rows.
                    plan.left_rows_to_join = LeftRowsToJoin::FirstRowOnly;
                    plan.right_rows_to_join = RightRowsToJoin::AllStoredRows;
                }
                else if (isInner(kind))
                {
                    /// `INNER ANY` returns a single pair: the first left row with the selected right row.
                    plan.left_rows_to_join = LeftRowsToJoin::FirstRowOnly;
                    plan.right_rows_to_join = RightRowsToJoin::SelectedRowOnly;
                }
                else
                {
                    /// `LEFT`/`FULL ANY`: every left row joins the selected right row.
                    plan.left_rows_to_join = LeftRowsToJoin::All;
                    plan.right_rows_to_join = RightRowsToJoin::SelectedRowOnly;
                }
                break;
            case JoinStrictness::RightAny:
                /// Old `ANY` (`any_join_distinct_right_table_keys`), mirrors `HashJoin`: it assumes distinct
                /// right keys and joins one right row to every left row for all kinds; `RIGHT`/`FULL` unmatched
                /// right rows are covered by `emit_unmatched_right_rows`.
                plan.left_rows_to_join = LeftRowsToJoin::All;
                plan.right_rows_to_join = RightRowsToJoin::SelectedRowOnly;
                break;
            case JoinStrictness::Semi:
                if (kind == JoinKind::Right)
                {
                    /// `RIGHT SEMI` preserves the right side: the first left row joins all right rows.
                    plan.left_rows_to_join = LeftRowsToJoin::FirstRowOnly;
                    plan.right_rows_to_join = RightRowsToJoin::AllStoredRows;
                }
                else
                {
                    /// `LEFT SEMI`: every left row once, with the selected right row.
                    plan.left_rows_to_join = LeftRowsToJoin::All;
                    plan.right_rows_to_join = RightRowsToJoin::SelectedRowOnly;
                }
                break;
            case JoinStrictness::Anti:
                /// A true constant predicate matches every left row, so `ANTI` suppresses all of them;
                /// In case of empty left/right side, emit_unmatched_* take care of the output.
                break;
            case JoinStrictness::Unspecified:
            case JoinStrictness::Asof:
                UNREACHABLE();
        }
    }

    /// The stored right blocks feed only the cartesian matched output and the unmatched-right padding; when
    /// neither can ever read them, store nothing. The selected-right-row modes keep their one row separately
    /// in `selected_right_row`, so beyond it only the right-side row count (`total_rows_to_join`)
    /// can still affect the output.
    const bool matched_output_reads_stored_rows = constant_predicate_value
        && plan.left_rows_to_join != LeftRowsToJoin::None
        && plan.right_rows_to_join == RightRowsToJoin::AllStoredRows;
    if (!matched_output_reads_stored_rows && !plan.emit_unmatched_right_rows)
        plan.store_right_rows = false;

    return plan;
}

bool ConstantJoin::addBlockToJoin(const Block & source_block, bool check_limits)
{
    return addBlockToJoin(source_block, source_block.rows(), check_limits);
}

bool ConstantJoin::addBlockToJoin(const Block & source_block, size_t num_rows, bool check_limits)
{
    const bool select_right_row_mode = plan.right_rows_to_join == OutputPlan::RightRowsToJoin::SelectedRowOnly;

    size_t rows = source_block.rows();
    if (rows == 0 && num_rows != 0 && !source_block.columns())
        rows = num_rows;

    total_rows_to_join += rows;

    const bool need_selected_right_row = rows && select_right_row_mode && (plan.select_last_right_row || !selected_right_row);
    const bool need_stored_right_rows = rows && plan.store_right_rows;

    if (!need_selected_right_row && !need_stored_right_rows)
    {
        /// The block can never be emitted; only the right-side row count is needed - `joinBlock` and
        /// `alwaysReturnsEmptySet` check whether the right side turned out empty.
        return true;
    }

    if (!memory_usage_before_adding_blocks)
        memory_usage_before_adding_blocks = JoinCommon::getCurrentQueryMemoryUsage();

    if (!need_stored_right_rows)
    {
        chassert(need_selected_right_row);

        auto selected_block = source_block.cloneWithCutColumns(plan.select_last_right_row ? rows - 1 : 0, 1);
        selected_block = JoinCommon::materializeColumnsFromRightBlock(std::move(selected_block), right_sample_block);
        assertBlocksHaveEqualStructureAllowReplicated(right_sample_block, selected_block, "joined block");

        if (shrink_blocks)
            selected_block = selected_block.shrinkToFit();

        selected_right_row.emplace(materializeSelectedRowColumns(selected_block.getColumns()), ScatteredBlock::Selector(1));
        return true;
    }

    auto block_to_save = JoinCommon::materializeColumnsFromRightBlock(source_block, right_sample_block);
    assertBlocksHaveEqualStructureAllowReplicated(right_sample_block, block_to_save, "joined block");

    if (shrink_blocks)
        block_to_save = block_to_save.shrinkToFit();

    if (need_selected_right_row)
    {
        /// The selected right row lives outside the stored blocks: the first row is captured once, while
        /// `select_last_right_row` keeps replacing it with the freshest one.
        selected_right_row.emplace(
            materializeSelectedRowColumns(
                block_to_save.cloneWithCutColumns(plan.select_last_right_row ? rows - 1 : 0, 1).getColumns()),
            ScatteredBlock::Selector(1));
    }

    /// A spilled block does not count against the in-memory size limits, so it also skips the check below.
    if (trySpillRightBlock(block_to_save))
        return true;

    storeRightBlock(std::move(block_to_save), rows);

    if (!check_limits)
        return true;

    return table_join->sizeLimits().check(getTotalRowCount(), getTotalByteCount(), "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
}

/// Streams the block to disk when the in-memory size limits would be exceeded, and keeps streaming from then on.
/// Blocks without columns never spill: the Native format cannot persist a bare row count, so they stay
/// in memory as row-count metadata.
bool ConstantJoin::trySpillRightBlock(const Block & block_to_save)
{
    if (block_to_save.columns() == 0 || !tmp_data)
        return false;

    size_t max_bytes_in_join = table_join->sizeLimits().max_bytes;
    size_t max_rows_in_join = table_join->sizeLimits().max_rows;
    bool limits_reached = (max_bytes_in_join && getTotalByteCount() + block_to_save.allocatedBytes() >= max_bytes_in_join)
        || (max_rows_in_join && getTotalRowCount() + block_to_save.rows() >= max_rows_in_join);

    if (!tmp_stream && !limits_reached)
        return false;

    if (!tmp_stream)
        tmp_stream.emplace(std::make_shared<const Block>(right_sample_block), tmp_data);

    tmp_stream.value()->write(block_to_save);
    return true;
}

void ConstantJoin::storeRightBlock(Block block_to_save, size_t rows)
{
    assertBlocksHaveEqualStructureAllowReplicated(right_sample_block, block_to_save, "joined block");

    size_t min_bytes_to_compress = table_join->crossJoinMinBytesToCompress();
    size_t min_rows_to_compress = table_join->crossJoinMinRowsToCompress();

    if ((min_bytes_to_compress && getTotalByteCount() >= min_bytes_to_compress)
        || (min_rows_to_compress && getTotalRowCount() >= min_rows_to_compress))
    {
        block_to_save = block_to_save.compress();
        have_compressed = true;
    }

    doDebugAsserts();
    right_blocks.emplace_back(block_to_save.getColumns(), ScatteredBlock::Selector(rows));
    allocated_size += right_blocks.back().allocatedBytes();
    in_memory_rows += rows;
    doDebugAsserts();

    size_t total_bytes = getTotalByteCount();
    shrinkStoredBlocksToFit(total_bytes);
}

void ConstantJoin::doDebugAsserts() const
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    size_t debug_allocated_size = 0;
    for (const auto & stored_block : right_blocks)
        debug_allocated_size += stored_block.allocatedBytes();

    if (allocated_size != debug_allocated_size)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "allocated_size != debug_allocated_size ({} != {})",
            allocated_size,
            debug_allocated_size);
#endif
}

size_t ConstantJoin::getTotalByteCount() const
{
    doDebugAsserts();
    return allocated_size;
}

void ConstantJoin::shrinkStoredBlocksToFit(size_t & total_bytes_in_join)
{
    if (shrink_blocks)
        return;

    Int64 current_memory_usage = JoinCommon::getCurrentQueryMemoryUsage();
    Int64 query_memory_usage_delta = current_memory_usage - memory_usage_before_adding_blocks;
    Int64 max_total_bytes_for_query = memory_usage_before_adding_blocks ? table_join->getMaxMemoryUsage() : 0;

    auto max_total_bytes_in_join = table_join->sizeLimits().max_bytes;

    shrink_blocks = (max_total_bytes_in_join && total_bytes_in_join > max_total_bytes_in_join / 2)
        || (max_total_bytes_for_query && query_memory_usage_delta > max_total_bytes_for_query / 2);
    if (!shrink_blocks)
        return;

    LOG_DEBUG(
        log,
        "Shrinking stored blocks, memory consumption is {} {} calculated by join, {} {} by memory tracker",
        ReadableSize(total_bytes_in_join),
        max_total_bytes_in_join ? fmt::format("/ {}", ReadableSize(max_total_bytes_in_join)) : "",
        ReadableSize(query_memory_usage_delta),
        max_total_bytes_for_query ? fmt::format("/ {}", ReadableSize(max_total_bytes_for_query)) : "");

    for (auto & stored_block : right_blocks)
    {
        doDebugAsserts();

        size_t old_size = stored_block.allocatedBytes();

        try
        {
            /// A compressed column is already stored in its most compact form (and does not support `cloneResized`).
            for (auto & column : stored_block.columns)
                if (!typeid_cast<const ColumnCompressed *>(column.get()))
                    column = column->cloneResized(column->size());

            stored_block.rebuildReplicatedColumns();
        }
        catch (...)
        {
            /// `cloneResized` allocates a compacted copy while memory is already tight, so an allocation
            /// failure here is possible. The exception should faild the query; no need for cleanup.
            LOG_WARNING(log, "Failed to shrink stored blocks: {}", getCurrentExceptionMessage(/*with_stacktrace=*/ false));
            throw;
        }

        size_t new_size = stored_block.allocatedBytes();

        if (old_size >= new_size)
        {
            if (allocated_size < old_size - new_size)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Blocks allocated size value is broken: blocks_allocated_size = {}, old_size = {}, new_size = {}",
                    allocated_size,
                    old_size,
                    new_size);

            allocated_size -= old_size - new_size;
        }
        else
            allocated_size += new_size - old_size;

        doDebugAsserts();
    }

    auto new_total_bytes_in_join = getTotalByteCount();
    Int64 new_current_memory_usage = JoinCommon::getCurrentQueryMemoryUsage();

    LOG_DEBUG(
        log,
        "Shrunk stored blocks {} freed ({} by memory tracker), new memory consumption is {} ({} by memory tracker)",
        ReadableSize(total_bytes_in_join - new_total_bytes_in_join),
        ReadableSize(current_memory_usage - new_current_memory_usage),
        ReadableSize(new_total_bytes_in_join),
        ReadableSize(new_current_memory_usage));

    total_bytes_in_join = new_total_bytes_in_join;
}

namespace
{

size_t storedBlockRows(const StoredBlock & stored_block)
{
    return stored_block.selector.size();
}

StoredBlock decompressStoredBlock(const StoredBlock & stored_block)
{
    Columns new_columns;
    new_columns.reserve(stored_block.columns.size());
    for (const auto & column : stored_block.columns)
        new_columns.emplace_back(column->decompress());

    return StoredBlock(std::move(new_columns), ScatteredBlock::Selector(storedBlockRows(stored_block)));
}

void insertRangeFromStoredBlock(MutableColumns & dst_columns, size_t dst_offset, const StoredBlock & stored_block, size_t start, size_t rows)
{
    for (size_t col_num = 0; col_num < stored_block.columns.size(); ++col_num)
    {
        if (const auto * replicated_column = stored_block.replicated_columns[col_num])
        {
            for (size_t row = start; row != start + rows; ++row)
                dst_columns[dst_offset + col_num]->insertFrom(
                    *replicated_column->getNestedColumn(),
                    replicated_column->getIndexes().getIndexAt(row));
        }
        else
            dst_columns[dst_offset + col_num]->insertRangeFrom(*stored_block.columns[col_num], start, rows);
    }
}

}

bool ConstantJoin::alwaysReturnsEmptySet() const
{
    /// The join emits rows in three ways; it is statically empty only when none of them can ever fire.
    /// The left-side cardinality is unknown here, so a behavior counts as possible if some cardinality
    /// triggers it: e.g. `RIGHT ANTI JOIN ... ON 1` emits right rows only when the left side turns out empty.
    const bool may_emit_matched_rows = constant_predicate_value && total_rows_to_join != 0
        && plan.left_rows_to_join != OutputPlan::LeftRowsToJoin::None;
    /// With a constant-true predicate and a non-empty right side every left row matches, so no left row is
    /// ever left unmatched. This makes `LEFT ANTI JOIN ... ON 1` provably empty once the right side is filled
    /// (the matched rows exist but `ANTI` suppresses them), letting `JoiningTransform` cancel the left side.
    const bool may_emit_unmatched_left_rows = plan.emit_unmatched_left_rows
        && !(constant_predicate_value && total_rows_to_join != 0);
    const bool may_emit_unmatched_right_rows = total_rows_to_join != 0 && plan.emit_unmatched_right_rows;

    return !may_emit_matched_rows && !may_emit_unmatched_left_rows && !may_emit_unmatched_right_rows;
}

/** Base of the `ConstantJoin` results: builds the output header, replicates the probe (left) rows, and chunks
  * the output by `max_joined_block_size_rows` / `max_joined_block_size_bytes`.
  * Each subclass emits one specific output shape; `ConstantJoin::joinBlock` decides which one to construct.
  * The probe-row cursor `left_row` makes `next` resumable: a result finishes only when the cursor reaches
  * the end of the probe block.
  */
class ConstantJoinResultBase : public IJoinResult
{
public:
    ConstantJoinResultBase(const ConstantJoin & join_, Block block_)
        : join(join_)
        , block(std::move(block_))
    {
        src_left_columns.reserve(block.columns());
        for (size_t i = 0; i != block.columns(); ++i)
        {
            const auto & left_column = block.getByPosition(i);
            result_sample.insert(left_column);
            src_left_columns.push_back(left_column.column.get());
        }

        for (const auto & right_column : join.right_sample_block)
            result_sample.insert(right_column);
    }

protected:
    /// Starts the next output block, reserving for the expected number of rows (`outputBlockIsFull` may still
    /// cut the block short). Without a row cap nothing is reserved: the cartesian estimate is not bounded by
    /// the probe block, while `max_joined_block_bytes` may still chunk the output into much smaller blocks,
    /// so reserving the full estimate could allocate far more than one output block ever holds.
    /// `HashJoin` skips reservation for `max_joined_block_rows = 0` the same way.
    void startOutputBlock(size_t expected_rows)
    {
        dst_columns = result_sample.cloneEmptyColumns();
        size_t to_reserve = join.max_joined_block_rows ? std::min(join.max_joined_block_rows, expected_rows) : 0;
        for (auto & dst : dst_columns)
            dst->reserve(to_reserve);
        rows_added = 0;
        bytes_added = 0;
    }

    bool outputBlockIsFull() const
    {
        return (join.max_joined_block_rows && rows_added > join.max_joined_block_rows)
            || (join.max_joined_block_bytes && bytes_added > join.max_joined_block_bytes);
    }

    /// Joins the probe row at `left_row` with the given right rows.
    void appendRightRowsForCurrentLeftRow(const StoredBlock & right_rows, size_t rows_right)
    {
        replicateCurrentLeftRow(rows_right);
        insertRangeFromStoredBlock(dst_columns, src_left_columns.size(), right_rows, 0, rows_right);
        accountAddedRows(rows_right);
    }

    JoinResultBlock finishOutputBlock()
    {
        bool is_last = left_row >= block.rows();
        auto res = result_sample.cloneWithColumns(std::move(dst_columns));
        return {res, nullptr, is_last};
    }

    const ConstantJoin & join;
    Block block;
    /// The result header and raw pointers to the left data columns; invariant across `next` calls.
    Block result_sample;
    ColumnRawPtrs src_left_columns;
    /// The probe-row cursor: `next` resumes from it when the previous output block was cut short.
    size_t left_row = 0;

private:
    void replicateCurrentLeftRow(size_t count)
    {
        for (size_t col_num = 0; col_num < src_left_columns.size(); ++col_num)
            dst_columns[col_num]->insertManyFrom(*src_left_columns[col_num], left_row, count);
    }

    void accountAddedRows(size_t count)
    {
        rows_added += count;

        if (!join.max_joined_block_bytes)
            return;

        bytes_added = 0;
        for (const auto & dst : dst_columns)
            bytes_added += dst->byteSize();
    }

    MutableColumns dst_columns;
    size_t rows_added = 0;
    size_t bytes_added = 0;
};

/// Nothing to emit for this probe block: the rows match but `ANTI` suppresses them, or nothing matches
/// and the kind has no left-side padding.
class ConstantJoinEmptyResult final : public ConstantJoinResultBase
{
public:
    /// The probe block is needed only for its structure - the output header must still contain its columns -
    /// so its data is dropped right away.
    ConstantJoinEmptyResult(const ConstantJoin & join_, const Block & block_)
        : ConstantJoinResultBase(join_, block_.cloneEmpty())
    {
    }

    JoinResultBlock next() override { return {result_sample.cloneEmpty(), nullptr, true}; }
};

/// Unmatched probe rows padded with right-side defaults (`LEFT`/`FULL`/`ANTI` kinds when nothing matches).
/// The output is the probe block itself with default right columns appended - every probe row is emitted
/// exactly once, in order, so when slicing is not needed the block is reused as-is.
class ConstantJoinUnmatchedLeftRowsResult final : public ConstantJoinResultBase
{
public:
    using ConstantJoinResultBase::ConstantJoinResultBase;

    JoinResultBlock next() override
    {
        const size_t rows_total = block.rows();

        size_t chunk_rows = rows_total - left_row;
        if (join.max_joined_block_rows)
            chunk_rows = std::min(chunk_rows, join.max_joined_block_rows);
        if (join.max_joined_block_bytes && rows_total)
        {
            /// The output size is probably dominated by the left columns: the appended right columns hold only
            /// default values. We could do more precise measurements, but the main idea behind max_joined_block_{rows,bytes}
            /// is to prevent cross-join explosions, so a rough estimate is enough to keep the output blocks small.
            size_t left_bytes_per_row = std::max<size_t>(1, block.bytes() / rows_total);
            chunk_rows = std::min(chunk_rows, std::max<size_t>(1, join.max_joined_block_bytes / left_bytes_per_row));
        }

        Columns res_columns;
        res_columns.reserve(result_sample.columns());

        for (size_t i = 0; i < src_left_columns.size(); ++i)
        {
            const auto & left_column = block.getByPosition(i).column;
            res_columns.push_back(chunk_rows == rows_total ? left_column : left_column->cut(left_row, chunk_rows));
        }

        for (const auto & right_column : join.right_sample_block)
        {
            auto default_column = right_column.column->cloneEmpty();
            JoinCommon::addDefaultValues(*default_column, right_column.type, chunk_rows);
            res_columns.push_back(std::move(default_column));
        }

        left_row += chunk_rows;
        bool is_last = left_row >= rows_total;
        return {result_sample.cloneWithColumns(res_columns), nullptr, is_last};
    }
};

/// Every probe row joined with the single selected right row (`RightRowsToJoin::SelectedRowOnly`).
/// Like `ConstantJoinUnmatchedLeftRowsResult`, the output is 1:1 with the probe block — every probe row is
/// emitted exactly once, in order — so the left columns are reused (or sliced when the block limits require
/// chunking), and each right column repeats the selected value with one bulk `insertManyFrom`.
class ConstantJoinSelectedRowResult final : public ConstantJoinResultBase
{
public:
    using ConstantJoinResultBase::ConstantJoinResultBase;

    JoinResultBlock next() override
    {
        const size_t rows_total = block.rows();

        chassert(join.selected_right_row);

        const Columns & selected_row = join.selected_right_row->columns;

        size_t chunk_rows = rows_total - left_row;
        if (join.max_joined_block_rows)
            chunk_rows = std::min(chunk_rows, join.max_joined_block_rows);
        if (join.max_joined_block_bytes && rows_total)
        {
            size_t selected_row_bytes = 0;
            for (const auto & column : selected_row)
                selected_row_bytes += column->byteSize();

            size_t bytes_per_row = std::max<size_t>(1, block.bytes() / rows_total + selected_row_bytes);
            chunk_rows = std::min(chunk_rows, std::max<size_t>(1, join.max_joined_block_bytes / bytes_per_row));
        }

        Columns res_columns;
        res_columns.reserve(result_sample.columns());

        for (size_t i = 0; i < src_left_columns.size(); ++i)
        {
            const auto & left_column = block.getByPosition(i).column;
            res_columns.push_back(chunk_rows == rows_total ? left_column : left_column->cut(left_row, chunk_rows));
        }

        for (const auto & selected_column : selected_row)
        {
            auto dst_column = selected_column->cloneEmpty();
            dst_column->reserve(chunk_rows);
            dst_column->insertManyFrom(*selected_column, 0, chunk_rows);
            res_columns.push_back(std::move(dst_column));
        }

        left_row += chunk_rows;
        bool is_last = left_row >= rows_total;
        return {result_sample.cloneWithColumns(res_columns), nullptr, is_last};
    }
};

/// Every probe row joined with every stored right row: the cartesian product.
/// Iterating the right side is resumable: when the output block fills up mid-way, `right_block_it`/`reader`
/// keep the position and the next call continues from the same probe row.
class ConstantJoinCartesianResult final : public ConstantJoinResultBase
{
public:
    using ConstantJoinResultBase::ConstantJoinResultBase;

    JoinResultBlock next() override
    {
        const size_t rows_total = block.rows();

        size_t expected_rows = 0;
        if (common::mulOverflow(rows_total, join.total_rows_to_join, expected_rows))
            expected_rows = join.max_joined_block_rows;
        startOutputBlock(expected_rows);

        for (; left_row < rows_total; ++left_row)
        {
            if (outputBlockIsFull())
                break;

            if (!right_block_it.has_value())
                right_block_it = join.right_blocks.begin();

            for (; *right_block_it != join.right_blocks.end(); ++*right_block_it)
            {
                if (outputBlockIsFull())
                    break;

                const auto & stored_block = **right_block_it;
                if (!join.have_compressed)
                    appendRightRowsForCurrentLeftRow(stored_block, storedBlockRows(stored_block));
                else
                    appendRightRowsForCurrentLeftRow(decompressStoredBlock(stored_block), storedBlockRows(stored_block));
            }

            if (*right_block_it != join.right_blocks.end())
                break;

            if (join.tmp_stream)
            {
                if (!reader)
                    reader = join.tmp_stream->getReadStream();

                while (reader)
                {
                    if (outputBlockIsFull())
                        break;

                    auto block_right = reader.value()->read();
                    if (block_right.empty())
                    {
                        reader.reset();
                        break;
                    }

                    appendRightRowsForCurrentLeftRow(StoredBlock(block_right.getColumns()), block_right.rows());
                }
            }

            if (reader)
                break;

            /// The right side is exhausted for this probe row; the next one starts over.
            right_block_it = std::nullopt;
        }

        return finishOutputBlock();
    }

private:
    std::optional<ConstantJoin::StoredBlocks::const_iterator> right_block_it;
    std::optional<TemporaryBlockStreamReaderHolder> reader;
};

/// The counterpart of `ConstantJoinUnmatchedLeftRowsResult` for the right side (`emit_unmatched_right_rows`):
/// when no probe row ever matched, `getNonJoinedBlocks` streams every stored right row — matching is
/// all-or-nothing under a constant predicate, so either all of them were claimed by the probe side or none were.
/// Unlike the results constructed by `joinBlock`, this is a `RightColumnsFiller`: it produces only the right
/// columns, and the wrapping `NotJoinedBlocks` pads them with left-side defaults.
/// The store is emitted in chunks of `max_block_size` rows: `right_block_it`/`reader` walk the in-memory and
/// spilled blocks, and `right_block_offset` resumes mid-block when a block is wider than one chunk
/// (a compressed or spilled block is decompressed/read once and held until it is fully emitted).
class ConstantJoinUnmatchedRightRowsFiller final : public NotJoinedBlocks::RightColumnsFiller
{
public:
    ConstantJoinUnmatchedRightRowsFiller(const ConstantJoin & join_, UInt64 max_block_size_)
        : join(join_)
        , max_block_size(max_block_size_ ? max_block_size_ : std::numeric_limits<UInt64>::max())
    {
    }

    Block getEmptyBlock() override { return join.right_sample_block.cloneEmpty(); }

    size_t fillColumns(MutableColumns & columns_right) override
    {
        size_t rows_added = 0;

        auto insert_rows = [&](const StoredBlock & stored_rows, size_t start, size_t rows)
        {
            insertRangeFromStoredBlock(columns_right, 0, stored_rows, start, rows);
            rows_added += rows;
        };

        if (!right_block_it)
            right_block_it = join.right_blocks.begin();

        for (; *right_block_it != join.right_blocks.end() && rows_added < max_block_size; ++*right_block_it)
        {
            const auto & stored_block = **right_block_it;
            size_t rows_available = storedBlockRows(stored_block) - right_block_offset;
            size_t rows_to_take = std::min<size_t>(rows_available, max_block_size - rows_added);
            if (rows_to_take == 0)
            {
                right_block_offset = 0;
                continue;
            }

            if (!join.have_compressed)
                insert_rows(stored_block, right_block_offset, rows_to_take);
            else
            {
                /// A block wider than `max_block_size` is emitted in several chunks: decompress it only once.
                if (!current_decompressed_block)
                    current_decompressed_block.emplace(decompressStoredBlock(stored_block));

                insert_rows(*current_decompressed_block, right_block_offset, rows_to_take);
            }

            right_block_offset += rows_to_take;
            if (right_block_offset != storedBlockRows(stored_block))
                return rows_added;

            right_block_offset = 0;
            current_decompressed_block.reset();
        }

        if (*right_block_it != join.right_blocks.end())
            return rows_added;

        if (!join.tmp_stream || spilled_blocks_exhausted)
            return rows_added;

        if (!reader)
            reader = join.tmp_stream->getReadStream();

        while (rows_added < max_block_size)
        {
            if (!current_spilled_block)
            {
                auto block_right = reader.value()->read();
                if (block_right.empty())
                {
                    /// The spilled rows are emitted exactly once: a re-created read stream would start over
                    /// from the beginning, so remember that the spill has been fully read.
                    reader.reset();
                    spilled_blocks_exhausted = true;
                    break;
                }

                current_spilled_block.emplace(block_right.getColumns(), ScatteredBlock::Selector(block_right.rows()));
            }

            size_t rows_available = storedBlockRows(*current_spilled_block) - right_block_offset;
            size_t rows_to_take = std::min<size_t>(rows_available, max_block_size - rows_added);
            if (rows_to_take == 0)
            {
                current_spilled_block.reset();
                right_block_offset = 0;
                continue;
            }

            insert_rows(*current_spilled_block, right_block_offset, rows_to_take);

            right_block_offset += rows_to_take;
            if (right_block_offset != storedBlockRows(*current_spilled_block))
                return rows_added;

            current_spilled_block.reset();
            right_block_offset = 0;
        }

        return rows_added;
    }

private:
    const ConstantJoin & join;
    UInt64 max_block_size;
    std::optional<ConstantJoin::StoredBlocks::const_iterator> right_block_it;
    size_t right_block_offset = 0;
    std::optional<StoredBlock> current_decompressed_block;
    std::optional<TemporaryBlockStreamReaderHolder> reader;
    std::optional<StoredBlock> current_spilled_block;
    /// Set when the spilled rows have been fully emitted; the read stream must not be re-created after that.
    bool spilled_blocks_exhausted = false;
};

JoinResultPtr ConstantJoin::joinBlock(Block block)
{
    /// A constant predicate cannot match rows of an empty right side; `alwaysReturnsEmptySet` applies the same rule.
    const bool has_match = constant_predicate_value && total_rows_to_join != 0;

    if (!has_match)
    {
        if (plan.emit_unmatched_left_rows)
            return std::make_unique<ConstantJoinUnmatchedLeftRowsResult>(*this, std::move(block));

        return std::make_unique<ConstantJoinEmptyResult>(*this, block);
    }

    if (block.rows())
    {
        if (plan.left_rows_to_join == OutputPlan::LeftRowsToJoin::FirstRowOnly)
        {
            /// Probe streams race here: exactly one matching block keeps its first row, the losers emit nothing.
            bool expected = false;
            if (has_seen_matching_rows.compare_exchange_strong(expected, true))
                block = block.cloneWithCutColumns(0, 1);
            else
                return std::make_unique<ConstantJoinEmptyResult>(*this, block);
        }
        else
            has_seen_matching_rows = true;
    }

    /// The left rows match, but `ANTI` kinds suppress matched rows.
    if (plan.left_rows_to_join == OutputPlan::LeftRowsToJoin::None)
        return std::make_unique<ConstantJoinEmptyResult>(*this, block);

    if (plan.right_rows_to_join == OutputPlan::RightRowsToJoin::SelectedRowOnly)
        return std::make_unique<ConstantJoinSelectedRowResult>(*this, std::move(block));

    return std::make_unique<ConstantJoinCartesianResult>(*this, std::move(block));
}

IBlocksStreamPtr ConstantJoin::getNonJoinedBlocks(const Block &, const Block & result_sample_block, UInt64 max_block_size) const
{
    if (total_rows_to_join == 0 || !plan.emit_unmatched_right_rows || has_seen_matching_rows.load())
        return {};

    auto filler = std::make_unique<ConstantJoinUnmatchedRightRowsFiller>(*this, max_block_size);
    size_t left_columns_count = result_sample_block.columns() - right_sample_block.columns();
    return std::make_shared<NotJoinedBlocks>(std::move(filler), result_sample_block, left_columns_count, *table_join);
}

}
