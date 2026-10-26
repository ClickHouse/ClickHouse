#pragma once

#include <limits>
#include <optional>

#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/SortedBlocksWriter.h>
#include <QueryPipeline/SizeLimits.h>
#include <Common/CacheBase.h>
#include <Common/SharedMutex.h>

namespace DB
{

class TableJoin;
class MergeJoinCursor;
struct MergeJoinEqualRange;
class RowBitmaps;
enum class JoinTableSide : uint8_t;

namespace JoinStuff
{
class JoinUsedFlags;
}

class MergeJoin : public IJoin
{
public:
    static constexpr Int32 nulls_direction = -1;

    /// `size_limits_override` replaces the `max_rows_in_join` / `max_bytes_in_join` limits of the
    /// TableJoin.
    MergeJoin(std::shared_ptr<TableJoin> table_join_, SharedHeader right_sample_block, std::optional<SizeLimits> size_limits_override = {});

    /// State of the order-preserving probe of one left block (`keep_left_block_order` mode).
    /// Filled by `matchInOrder`, drained by `emitInOrder`, and carried across continuation
    /// chunks inside `NotProcessed`.
    struct OrderedProbeState
    {
        /// `right_block` of a range gathered into `gathered_right_columns`: `right_start` then
        /// indexes the gathered storage instead of a right block. SIZE_MAX blocks cannot exist,
        /// so the sentinel needs no reserved-value checks.
        static constexpr size_t gathered_block = std::numeric_limits<size_t>::max();

        /// `row_groups` value of a left row without matches. Group ids are bounded by the rows
        /// of one block, so the sentinel is unreachable.
        static constexpr size_t no_group = std::numeric_limits<size_t>::max();

        struct RightRange
        {
            size_t right_block;
            size_t right_start;
            size_t right_length;
        };

        /// Match lists shared by groups of left rows. All rows of one sorted equal-key span
        /// match the same right ranges, so they share one list instead of each row holding a
        /// copy - a per-row layout would make L equal-key rows matching ranges in B right blocks
        /// allocate L * B entries before any output is emitted, while this one stays linear in
        /// the input sizes. Group g matches `ranges[group_offsets[g] .. group_offsets[g + 1])`,
        /// ordered by right block and right row; the original left row r belongs to the group
        /// `row_groups[r]` (`no_group` when the row has no matches).
        std::vector<RightRange> ranges;
        std::vector<size_t> group_offsets;
        std::vector<size_t> row_groups;

        /// When the right side is on disk, `matchInOrder` copies the matched right rows out of
        /// every block while it is loaded for the matching anyway (within the memory budget), so
        /// `emitInOrder` does not have to load right blocks at all - the emission order follows
        /// the original left rows, whose matches can hop between blocks arbitrarily. Holds the
        /// `right_columns_to_add` columns of the gathered rows, referenced by the ranges with
        /// `right_block == gathered_block`. Gathering is a storage optimization only: ranges
        /// that stay block-referenced (past the budget) are emitted through the block cache at
        /// exactly the same output positions.
        Columns gathered_right_columns;

        /// Continuation position: the next output row is the original left row `next_row` paired
        /// with `ranges[next_range]` at `range_offset`.
        size_t next_row = 0;
        size_t next_range = 0;
        size_t range_offset = 0;
    };

    struct NotProcessed
    {
        Block block;
        size_t left_position{};
        size_t left_key_tail{};
        size_t right_position{};
        size_t right_block{};
        /// Set only by the order-preserving probe.
        std::shared_ptr<OrderedProbeState> ordered_state{};

        bool empty() const { return block.empty(); }
    };

    std::string getName() const override { return "PartialMergeJoin"; }

    std::string getAlgorithm() const override { return toString(JoinAlgorithm::PARTIAL_MERGE); }
    /// The classic probe re-sorts left blocks by the join key (once per right block), so it does
    /// not preserve the left stream's original order. The order-preserving probe
    /// (`keep_left_block_order`) matches on a sorted copy of the keys and emits the output in
    /// the original row order instead.
    bool preservesLeftBlockOrder() const override { return keep_left_block_order; }
    bool canPreserveLeftBlockOrder() const override;
    void keepLeftPipelineInOrder() override;
    const TableJoin & getTableJoin() const override { return *table_join; }
    bool addBlockToJoin(const Block & block, size_t num_rows, JoinBuildContext context) override;
    void checkTypesOfKeys(const Block & block) const override;
    JoinResultPtr joinBlock(Block block) override;
    void joinBlock(Block & block, std::optional<MergeJoin::NotProcessed> & not_processed);

    void setTotals(const Block &) override;

    bool hasPostBuildPhase() const override { return true; }
    void runPostBuildPhase() override;

    size_t getTotalRowCount() const override { return right_blocks.row_count; }
    size_t getTotalByteCount() const override { return right_blocks.bytes; }
    /// Has to be called only after `runPostBuildPhase`
    bool alwaysReturnsEmptySet() const override
    {
        return (is_right || is_inner || (is_left && is_semi_join)) && min_max_right_blocks.empty();
    }

    IBlocksStreamPtr getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const override;

    StepAnalysisReport getAnalysisReport() const override;

    /// Called by `SpillingHashJoin` before draining the collected right blocks into this join:
    /// the data already exceeds the external-join threshold, so the right side goes to disk right
    /// away instead of waiting for the size limits to trip.
    void spillRightBlocksToDisk();

    static bool isSupported(const std::shared_ptr<TableJoin> & table_join);
    static bool isSupported(JoinKind kind, JoinStrictness strictness);

private:
    friend class NotJoinedMerge;

    struct RightBlockInfo
    {
        std::shared_ptr<Block> block;
        size_t block_number;
        size_t & skip;
        RowBitmaps * bitmaps;
        std::unique_ptr<std::vector<bool>> used_bitmap;

        RightBlockInfo(std::shared_ptr<Block> block_, size_t block_number, size_t & skip_, RowBitmaps * bitmaps);
        ~RightBlockInfo(); /// apply used bitmap
        void setUsed(size_t start, size_t length);
    };

    /// There're two size limits for right-hand table: max_rows_in_join, max_bytes_in_join.
    /// max_bytes is preferred. If it isn't set we approximate it as (max_rows * bytes/row).
    struct BlockByteWeight
    {
        size_t operator()(const Block & block) const { return block.bytes(); }
    };

    using Cache = CacheBase<size_t, Block, std::hash<size_t>, BlockByteWeight>;

    mutable SharedMutex rwlock;
    std::shared_ptr<TableJoin> table_join;
    SizeLimits size_limits;
    SortDescription left_sort_description;
    SortDescription right_sort_description;
    SortDescription left_merge_description;
    SortDescription right_merge_description;
    Block right_sample_block;
    Block right_table_keys;
    /// Columns from right side of join, both key and additional
    Block right_columns_to_add;
    SortedBlocksWriter::Blocks right_blocks;

    Names key_names_right;
    Names key_names_left;

    /// Additional conditions for rows to join from JOIN ON section.
    /// Only rows where conditions are met can be joined.
    String mask_column_name_left;
    String mask_column_name_right;

    Blocks min_max_right_blocks;
    std::shared_ptr<SortedBlocksBuffer> left_blocks_buffer;
    std::shared_ptr<RowBitmaps> used_rows_bitmap;
    /// INNER ANY must emit one row per KEY, not one row per matching left row - the semantics
    /// `HashJoin` implements with a claim-once flag per hash-table entry. Here the claim target
    /// is the key's globally first right row (every probe scans the sorted right blocks in order,
    /// so all probes resolve one key to the same block and row): per-row atomic flags over the
    /// final sorted right blocks, shared by all probe streams, one byte per right row. Allocated
    /// by `runPostBuildPhase` only for INNER ANY; null otherwise.
    std::shared_ptr<JoinStuff::JoinUsedFlags> inner_any_used_flags;
    /// Row counts of the final sorted right blocks, recorded while they stream through the merge
    /// (only for INNER ANY): the spilled blocks are not loaded again before the probe, so this is
    /// the only place their sizes are visible for sizing `inner_any_used_flags`.
    std::vector<size_t> right_blocks_row_counts;
    mutable std::unique_ptr<Cache> cached_right_blocks;
    /// Byte cap of the per-left-block gather buffer of the order-preserving probe, set by
    /// `mergeFlushedRightBlocks` to half of the probe memory limit (whose full value sizes
    /// `cached_right_blocks`). A hard bound: every match run is measured before it is copied,
    /// and a run that would cross the cap stops the gathering (the spilled right blocks are
    /// row-capped, so a block-granular check could overshoot by one arbitrarily wide block).
    /// Each probe stream has its own buffer, so the peak is one cap of shrunk data per
    /// stream - transiently about twice that while the gathered columns grow.
    size_t gather_max_bytes = 0;
    std::vector<std::shared_ptr<Block>> loaded_right_blocks;
    std::unique_ptr<SortedBlocksWriter> disk_writer;
    /// Set of files with sorted blocks
    SortedBlocksWriter::SortedFiles flushed_right_blocks;
    std::atomic<bool> is_in_memory{true};
    const bool is_any_join;
    const bool is_all_join;
    const bool is_semi_join;
    const bool is_inner;
    const bool is_left;
    const bool is_right;
    const bool is_full;
    bool keep_left_block_order = false;
    const size_t max_joined_block_rows;
    /// Byte cap of one output chunk, honored by the order-preserving probe (the classic probe
    /// splits by rows only). The hash join it replaces under `SpillingHashJoin` honors it too.
    const size_t max_joined_block_bytes;
    const size_t max_rows_in_right_block;
    const size_t max_files_to_merge;

    /// Non-atomic is safe: the standalone build side is single threaded, and `SpillingHashJoin`
    /// serializes its drain into this join with `merge_join_insert_mutex` (which also provides
    /// the happens-before ordering for these counters).
    UInt64 build_sort_time_ns = 0;
    /// Written only by the post-build phase, which runs once.
    UInt64 right_spilled_compressed_bytes = 0;
    std::atomic<UInt64> probe_sort_time_ns{0};
    std::atomic<UInt64> total_left_rows{0};
    std::atomic<UInt64> matched_left_rows{0};

    Names lowcard_right_keys;

    LoggerPtr log;

    void changeLeftColumns(Block & block, MutableColumns && columns) const;
    void addRightColumns(Block & block, MutableColumns && columns);

    template <bool is_all>
    std::optional<NotProcessed> extraBlock(Block & processed, MutableColumns && left_columns, MutableColumns && right_columns,
                             size_t left_position, size_t left_key_tail, size_t right_position,
                             size_t right_block_number);

    void mergeRightBlocks();

    template <bool in_memory>
    size_t rightBlocksCount() const;
    template <bool in_memory, bool is_all>
    void joinSortedBlock(Block & block, std::optional<NotProcessed> & not_processed);
    template <bool in_memory>
    std::shared_ptr<Block> loadRightBlock(size_t pos) const;

    std::shared_ptr<Block> getRightBlock(size_t pos) const
    {
        if (is_in_memory)
            return loadRightBlock<true>(pos);
        return loadRightBlock<false>(pos);
    }

    size_t getRightBlocksCount() const
    {
        if (is_in_memory)
            return rightBlocksCount<true>();
        return rightBlocksCount<false>();
    }

    void joinBlockInOrder(Block & block, std::optional<NotProcessed> & not_processed);
    std::shared_ptr<OrderedProbeState> matchInOrder(const Block & block);
    void emitInOrder(Block & block, const std::shared_ptr<OrderedProbeState> & state, std::optional<NotProcessed> & not_processed);

    template <bool is_all> /// ALL or ANY
    bool leftJoin(MergeJoinCursor & left_cursor, const Block & left_block, RightBlockInfo & right_block_info,
                  MutableColumns & left_columns, MutableColumns & right_columns, size_t & left_key_tail, size_t & matched_rows);
    bool semiLeftJoin(MergeJoinCursor & left_cursor, const Block & left_block, RightBlockInfo & right_block_info,
                  MutableColumns & left_columns, MutableColumns & right_columns, size_t & matched_rows);
    bool allInnerJoin(MergeJoinCursor & left_cursor, const Block & left_block, RightBlockInfo & right_block_info,
                  MutableColumns & left_columns, MutableColumns & right_columns, size_t & left_key_tail, size_t & matched_rows);

    Block modifyRightBlock(const Block & src_block) const;
    bool saveRightBlock(Block && block);

    void mergeInMemoryRightBlocks();
    void mergeFlushedRightBlocks();

    void initRightTableWriter();

    bool needConditionJoinColumn() const;
    void addConditionJoinColumn(Block & block, JoinTableSide block_side) const;
};

}
