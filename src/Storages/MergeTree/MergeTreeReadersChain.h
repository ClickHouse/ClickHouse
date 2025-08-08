#pragma once
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/PatchParts/MergeTreePatchReader.h>

namespace DB
{

using RangeReaders = std::vector<MergeTreeRangeReader>;

class MergeTreeReadersChain
{
public:
    MergeTreeReadersChain() = default;
    MergeTreeReadersChain(RangeReaders range_readers_, MergeTreePatchReaders patch_readers_);
    bool isInitialized() const { return is_initialized; }

    using ReadResult = MergeTreeRangeReader::ReadResult;
    ReadResult read(size_t max_rows, MarkRanges & ranges, std::vector<MarkRanges> & patch_ranges);

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

    void applyPatches(
        const Block & result_header,
        Columns & result_columns,
        Block & versions_block,
        std::optional<UInt64> min_version,
        std::optional<UInt64> max_version,
        bool after_conversions,
        const Block & additional_columns) const;

    using PatchReadResultPtr = MergeTreePatchReader::PatchReadResultPtr;

    RangeReaders range_readers;
    MergeTreePatchReaders patch_readers;
    std::vector<std::deque<PatchReadResultPtr>> patches_results;

    bool is_initialized = false;
    LoggerPtr log = getLogger("MergeTreeReadersChain");
};

};
