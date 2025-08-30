#pragma once

#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeIndexReader.h>
#include <Storages/MergeTree/MergeTreeIndices.h>

namespace DB
{

struct MergeTreeIndexReadResult;
using MergeTreeIndexReadResultPtr = std::shared_ptr<MergeTreeIndexReadResult>;

class MergeTreeReaderTextIndex : public IMergeTreeReader
{
public:
    using MatchingMarks = std::vector<bool>;

    MergeTreeReaderTextIndex(const IMergeTreeReader * main_reader_, MergeTreeIndexWithCondition index_);

    size_t readRows(
        size_t from_mark,
        size_t current_task_last_mark,
        bool continue_reading,
        size_t max_rows_to_read,
        size_t offset,
        Columns & res_columns) override;

    bool canReadIncompleteGranules() const override { return main_reader->canReadIncompleteGranules(); }

    bool canSkipMark(size_t mark) override;

private:
    /// Delegates to the main reader to determine if reading incomplete index granules is supported.
    const IMergeTreeReader * main_reader;

    MergeTreeIndexWithCondition index;

    MarkRanges index_ranges;

    std::optional<MergeTreeIndexReader> index_reader;

    /// Current row position used when continuing reads across multiple calls.
    size_t current_row = 0;

    struct GranuleWithResult
    {
        MergeTreeIndexGranulePtr granule;
        bool may_be_true = true;
    };

    std::map<size_t, GranuleWithResult> cached_granules;
};

}
