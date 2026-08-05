#pragma once

#include <atomic>
#include <list>
#include <memory>
#include <optional>

#include <Core/Block.h>
#include <Core/Joins.h>
#include <Interpreters/HashJoin/ScatteredBlock.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Common/Logger.h>

namespace DB
{

class TableJoin;

/** Implements joins with constant predicates, including `CROSS JOIN` and comma joins.
  * It stores right-side blocks without hash keys and emits cartesian or default rows according to the join kind,
  * strictness, and constant predicate value.
  * Right-side blocks may be compressed or spilled to a temporary block stream when join limits are exceeded.
  */
class ConstantJoin final : public IJoin
{
public:
    ConstantJoin(std::shared_ptr<TableJoin> table_join_, SharedHeader right_sample_block_, bool any_take_last_row_ = false);

    std::string getName() const override { return "ConstantJoin"; }
    const TableJoin & getTableJoin() const override { return *table_join; }

    bool isCloneSupported() const override
    {
        return getTotals().empty() && total_rows_to_join == 0;
    }

    std::shared_ptr<IJoin> clone(
        const std::shared_ptr<TableJoin> & table_join_,
        SharedHeader,
        SharedHeader right_sample_block_) const override
    {
        return std::make_shared<ConstantJoin>(table_join_, right_sample_block_, any_take_last_row);
    }

    bool addBlockToJoin(const Block & source_block, bool check_limits) override;
    bool addBlockToJoin(const Block & source_block, size_t num_rows, bool check_limits) override;

    void checkTypesOfKeys(const Block &) const override {}

    JoinResultPtr joinBlock(Block block) override;

    size_t getTotalRowCount() const override { return in_memory_rows; }
    size_t getTotalByteCount() const override;

    bool alwaysReturnsEmptySet() const override;

    IBlocksStreamPtr getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const override;

private:
    friend class ConstantJoinResultBase;
    friend class ConstantJoinUnmatchedLeftRowsResult;
    friend class ConstantJoinSelectedRowResult;
    friend class ConstantJoinCartesianResult;
    friend class ConstantJoinUnmatchedRightRowsFiller;

    /// `ConstantJoin` stores whole blocks, so the selector of every `StoredBlock` is the trivial full range
    /// and serves as the row count — the columns cannot carry it when the query needs only the right-side
    /// row count and the block has no columns.
    using StoredBlocks = std::list<StoredBlock>;

    /** With a constant predicate the join kind, strictness, and predicate value fully determine how blocks are
      * stored and joined, so the whole strategy is fixed at construction time (see `makeOutputPlan`).
      * Only two facts are discovered at runtime, and they merely select between the precomputed behaviors:
      * whether the right side ended up empty (matched output needs right rows to join)
      * and whether any probe row matched (`has_seen_matching_rows`, gating the unmatched right rows).
      *
      * NOTE: RightRowsToJoin::SelectedRowOnly && store_right_rows is a special case, only necessary for the legacy
      * RightAny join (any_join_distinct_right_table_keys = 1).
      */
    struct OutputPlan
    {
        /// The left rows joined with the right side when the predicate matches and the right side is not empty.
        enum class LeftRowsToJoin : uint8_t
        {
            None,
            All,
            /// The first probe row over all probe streams; implemented by `joinBlock` cutting the probe block.
            FirstRowOnly,
        };

        /// The right rows joined to every left row selected by `left_rows_to_join`.
        enum class RightRowsToJoin : uint8_t
        {
            AllStoredRows,
            /// One row chosen at build time (see `select_last_right_row`), always kept in
            /// `selected_right_row`, separately from the stored blocks.
            SelectedRowOnly,
        };


        LeftRowsToJoin left_rows_to_join = LeftRowsToJoin::None;
        RightRowsToJoin right_rows_to_join = RightRowsToJoin::AllStoredRows;
        bool store_right_rows = true;
        /// `LEFT`/`FULL`/`ANTI`: when nothing matches, emit the left rows with right-side defaults.
        bool emit_unmatched_left_rows = false;
        /// `RIGHT`/`FULL`/`ANTI`: when no left row matched, emit the stored right rows with left-side defaults.
        bool emit_unmatched_right_rows = false;
        /// Which right row `SelectedRowOnly` refers to: the first one, or the last one (`join_any_take_last_row`).
        bool select_last_right_row = false;
    };

    static OutputPlan makeOutputPlan(JoinKind kind, JoinStrictness strictness, bool constant_predicate_value, bool any_take_last_row);

    std::shared_ptr<TableJoin> table_join;

    /// Header of the right columns appended to every output row and the sample that stored right blocks normalize to.
    Block right_sample_block;

    /// Right-side data kept in memory, possibly compressed.
    StoredBlocks right_blocks;

    /// Spilling of right blocks once the join size limits are approached; `tmp_stream` exists after the first spill.
    TemporaryDataOnDiskScopePtr tmp_data;
    std::optional<TemporaryBlockStreamHolder> tmp_stream;

    /// All right rows, including spilled ones.
    size_t total_rows_to_join = 0;
    /// Rows residing in `right_blocks` only.
    size_t in_memory_rows = 0;
    /// Incrementally maintained sum of `StoredBlock::allocatedBytes` over `right_blocks`.
    size_t allocated_size = 0;
    /// At least one stored block is compressed; readers then decompress every stored block.
    bool have_compressed = false;
    /// The single right row joined by `RightRowsToJoin::SelectedRowOnly`; kept separately from the stored
    /// blocks and always materialized (a replicated one-row copy would pin the source block's nested columns).
    std::optional<StoredBlock> selected_right_row;

    /// Value of the constant join predicate; unconditionally true for explicit cartesian joins.
    const bool constant_predicate_value;
    /// How the right side is stored and how output rows are produced; fixed at construction.
    const OutputPlan plan;
    /// Whether any probe rows have matched; gates the unmatched right rows and the first-left-row cut in `joinBlock`.
    std::atomic_bool has_seen_matching_rows = false;
    /// The `join_any_take_last_row` setting; folded into `plan`, kept only for `clone`.
    const bool any_take_last_row;

    /// Per-result-block limits: `max_joined_block_size_rows` / `max_joined_block_size_bytes`.
    size_t max_joined_block_rows = 0;
    size_t max_joined_block_bytes = 0;

    /// Set under memory pressure; from then on stored blocks are shrunk to fit (see `shrinkStoredBlocksToFit`).
    bool shrink_blocks = false;
    /// Query memory usage at the first `addBlockToJoin`; the baseline for measuring the join's own consumption.
    Int64 memory_usage_before_adding_blocks = 0;

    LoggerPtr log;

    bool trySpillRightBlock(const Block & block_to_save);
    void storeRightBlock(Block block_to_save, size_t rows);
    void shrinkStoredBlocksToFit(size_t & total_bytes_in_join);
    void doDebugAsserts() const;
};

}
