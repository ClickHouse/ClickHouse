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

class MergeTreeReadersChain
{
    using DataflowCacheUpdateCallback
        = std::function<void(const ColumnsWithTypeAndName & columns, size_t read_bytes, std::optional<bool> & should_continue_sampling)>;

public:
    MergeTreeReadersChain() = default;
    MergeTreeReadersChain(RangeReaders range_readers_, MergeTreePatchReaders patch_readers_);
    bool isInitialized() const { return is_initialized; }

    using ReadResult = MergeTreeRangeReader::ReadResult;

    ReadResult
    read(size_t max_rows, MarkRanges & ranges, std::vector<MarkRanges> & patch_ranges, const DataflowCacheUpdateCallback & update_cb = {});

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
        size_t num_read_rows) const;

    void executePrewhereActions(
        MergeTreeRangeReader & reader,
        ReadResult & result,
        const Block & previous_header,
        bool is_last_reader);

    void readPatches(const Block & result_header, std::vector<MarkRanges> & patch_ranges, ReadResult & read_result);
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
    MergeTreePatchReaders patch_readers;
    std::vector<std::deque<PatchReadResultPtr>> patches_results;

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

    bool is_initialized = false;
    LoggerPtr log = getLogger("MergeTreeReadersChain");
};

};
