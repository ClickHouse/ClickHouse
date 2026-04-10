#pragma once
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/PatchParts/applyPatchesUtils.h>

#include <functional>

namespace DB
{

using RangeReaders = std::vector<MergeTreeRangeReader>;

class MergeTreeReadersChain
{
    using DataflowCacheUpdateCallback
        = std::function<void(const ColumnsWithTypeAndName & columns, size_t read_bytes, std::optional<bool> & should_continue_sampling)>;

public:
    MergeTreeReadersChain() = default;
    MergeTreeReadersChain(RangeReaders range_readers_, MergeTreePatchReaders patch_readers_,
                          bool preserve_last_reader_additional_columns_ = false);
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
    void addPatchVirtuals(ReadResult & result, const Block & header) const;
    void applyPatchesAfterReader(ReadResult & result, size_t reader_index);

    RangeReaders range_readers;
    MergeTreePatchReaders patch_readers;
    std::vector<std::deque<PatchReadResultPtr>> patches_results;

    bool is_initialized = false;

    /// When true, the last reader in the chain still saves projected-out columns
    /// in ReadResult::additional_columns. Needed by the pipelined reader where the
    /// downstream RestColumnsTransform requires them for DEFAULT expression evaluation.
    bool preserve_last_reader_additional_columns = false;

    LoggerPtr log = getLogger("MergeTreeReadersChain");
};

};
