#pragma once
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/PatchParts/MergeTreePatchReader.h>

#include <functional>

namespace DB
{

using RangeReaders = std::vector<MergeTreeRangeReader>;

struct ColumnForPatch
{
    enum class Order
    {
        /// Apply patch before converting the column to actual type.
        BeforeConversions,
        /// Apply patch after converting the column to actual type.
        AfterConversions,
        /// Apply patch after evaluating missing defaults for the column.
        AfterEvaluatingDefaults,
    };

    ColumnForPatch(const String & column_name_, Order order_) : column_name(column_name_), order(order_) {}

    String column_name;
    Order order;
};

using ColumnsForPatch = std::vector<ColumnForPatch>;
using ColumnsForPatches = std::vector<ColumnsForPatch>;

/// Cached for the same reason as `getMergeTreeRangeReaderLogger`: a readers chain is built for every
/// read task, and `getLogger` would take a process-global mutex each time.
LoggerPtr getMergeTreeReadersChainLogger();

class MergeTreeReadersChain
{
    using DataflowCacheUpdateCallback = std::function<void(
        const ColumnsWithTypeAndName & columns,
        const NameSet & partially_read_columns,
        size_t read_bytes,
        std::optional<bool> & should_continue_sampling)>;

public:
    MergeTreeReadersChain() = default;
    MergeTreeReadersChain(RangeReaders range_readers_, MergeTreePatchReaders patch_readers_);
    bool isInitialized() const { return is_initialized; }

    using ReadResult = MergeTreeRangeReader::ReadResult;

    ReadResult
    read(size_t max_rows, MarkRanges & ranges, std::vector<MarkRanges> & patch_ranges, const DataflowCacheUpdateCallback & update_cb = {},
         size_t max_block_size_rows = 0);

    size_t numReadRowsInCurrentGranule() const;
    size_t numPendingRowsInCurrentGranule() const;
    size_t numRowsInCurrentGranule() const;
    size_t currentMark() const;

    const Block & getSampleBlock() const;
    bool isCurrentRangeFinished() const;

private:
    /// Executes actions required before PREWHERE, such as alter conversions and filling defaults.
    void executeActionsBeforePrewhere(
        ReadResult & result,
        Columns & read_columns,
        MergeTreeRangeReader & range_reader,
        const Block & previous_header,
        const NameSet & previous_step_columns,
        size_t num_read_rows) const;

    /// Evaluates default expressions for columns that are absent in part.
    void evaluateMissingDefaults(
        MergeTreeRangeReader & range_reader,
        const ReadResult & result,
        const Block & previous_header,
        Columns & columns) const;

    void executePrewhereActions(
        MergeTreeRangeReader & reader,
        ReadResult & result,
        const Block & previous_header,
        bool is_last_reader,
        bool keep_additional_columns);

    void readPatches(const Block & result_header, std::vector<MarkRanges> & patch_ranges, ReadResult & read_result);

    /// Materializes the sort-key result columns of MergeOnKey patches.
    /// Returns the main block used for key comparisons.
    Block executeSortingKeyExpressions(const Block & result_header, ReadResult & read_result);

    void addPatchVirtuals(Block & to, const Block & from) const;
    void addPatchVirtuals(ReadResult & result, const Block & header) const;
    void applyPatchesAfterReader(ReadResult & result, size_t reader_index);
    ColumnsForPatches getColumnsForPatches(const Block & header, const Columns & columns) const;

    void applyPatches(
        const Block & result_header,
        Columns & result_columns,
        Block & versions_block,
        std::optional<UInt64> min_version,
        std::optional<UInt64> max_version,
        const ColumnsForPatches & columns_for_patches,
        const std::set<ColumnForPatch::Order> & suitable_orders,
        const Block & additional_columns) const;

    RangeReaders range_readers;
    /// The readers of `range_readers` that read columns, not only a filter: see `MergeTreeRangeReader::startReadingChain`.
    std::vector<IMergeTreeReader *> columns_cache_readers;
    MergeTreePatchReaders patch_readers;
    std::vector<std::deque<PatchReadResultPtr>> patches_results;

    /// For every reader, the names of the columns produced by the previous steps of the chain.
    /// They don't change between blocks, so they are collected once.
    std::vector<NameSet> previous_steps_columns;

    /// The last reader that may evaluate defaults of columns missing in the part, 0 if none.
    /// Only the steps before it keep the columns they project out.
    size_t last_reader_evaluating_defaults = 0;

    /// Storage names of overwritten columns that an on-fly MUTATION step genuinely consumes
    /// as a function input before any step overwrites them. They must still be converted to
    /// the post-`MODIFY` metadata type, otherwise the consuming action sees a type/storage
    /// mismatch. The keep-old fallback of an `UPDATE` (`if(cond, expr, col)`'s third argument)
    /// counts as a genuine consume too; it is ignored ONLY when `cond` is a compile-time
    /// constant-true, because then `FunctionIf` never reads the on-disk `col`. A real same-step
    /// read such as `UPDATE col = f(col)` always forces conversion. Columns whose sole reference
    /// is a constant-true fallback stay skipped; query PREWHERE steps are excluded (they convert
    /// at their own turn).
    /// See `collectColumnsConsumedByChainActions` and `executeActionsBeforePrewhere`.
    NameSet columns_consumed_by_chain_actions;

    /// PREWHERE granule ProfileEvents (GranulesReadByPrewhereReaders / GranulesSkippedByPrewhereReaders).
    /// Every granule PREWHERE reads is fed in mark order from the final result of each block.
    /// `GranulesReadByPrewhereReaders` is incremented as soon as a granule is first seen, so a query that
    /// stops early (e.g. a LIMIT) still counts the granules it read. The granule is kept pending until a
    /// higher mark (or the end of the task) proves it was read in full; only then, if no row survived, is it
    /// counted in `GranulesSkippedByPrewhereReaders` -- the unread remainder of a granule cut off by early
    /// termination is never declared dropped. A granule split across blocks repeats with the same mark; its
    /// survivor flags are OR-ed and it is counted once.
    void feedPrewhereGranuleMark(size_t mark, bool survived);
    void finalizeCompletedPrewhereGranule();

    bool prewhere_granule_stats_enabled = false;
    /// Index in `range_readers` of the first reader whose step is the query `PREWHERE`. Granules are counted
    /// as entering PREWHERE from the survivors at this reader's input, so filtering done by earlier readers
    /// (on-fly mutations, row-level policies) is not attributed to PREWHERE. Valid only when stats enabled.
    size_t first_query_prewhere_reader_index = 0;
    /// Index in `range_readers` of the last reader whose step is the query `PREWHERE`. Per-granule drop
    /// attribution walks every reader up to this one and stops here: a granule still non-empty at this point is
    /// never dropped by PREWHERE, whatever a later synthesized dynamic filter (a join runtime filter
    /// `optimizePrewhere` appends after the user predicate) does to it. Valid only when stats enabled.
    size_t last_query_prewhere_reader_index = 0;
    std::optional<size_t> prewhere_pending_mark;
    bool prewhere_pending_mark_survived = false;

    bool is_initialized = false;
    LoggerPtr log = getMergeTreeReadersChainLogger();
};

};
