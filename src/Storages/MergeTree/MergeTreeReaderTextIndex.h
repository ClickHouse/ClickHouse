#pragma once

#include <Interpreters/GinQueryString.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeIndexReader.h>
#include <Storages/MergeTree/MergeTreeIndices.h>

namespace DB
{

struct MergeTreeIndexReadResult;
using MergeTreeIndexReadResultPtr = std::shared_ptr<MergeTreeIndexReadResult>;
static constexpr std::string_view TEXT_INDEX_VIRTUAL_COLUMN_PREFIX = "__text_index_";

class MergeTreeReaderTextIndex : public IMergeTreeReader
{
public:
    using MatchingMarks = std::vector<bool>;

    MergeTreeReaderTextIndex(const IMergeTreeReader * main_reader_, NamesAndTypesList columns_, MergeTreeIndexWithCondition index_);

    size_t readRows(
        size_t from_mark,
        size_t current_task_last_mark,
        bool continue_reading,
        size_t max_rows_to_read,
        size_t offset,
        Columns & res_columns) override;

    bool canSkipMark(size_t mark) override;
    bool canReadIncompleteGranules() const override { return false; }

private:
    struct Granule
    {
        MergeTreeIndexGranulePtr granule;
        absl::flat_hash_map<StringRef, ColumnPtr> postings;
        bool may_be_true = true;
        bool need_read_postings = true;
    };

    void readPostingsIfNeeded(Granule & granule);
    void fillSkippedColumn(IColumn & column, size_t num_rows);
    void fillColumn(IColumn & column, const Granule & granule, TextSearchMode search_mode, size_t granule_offset, size_t num_rows);

    /// Delegates to the main reader to determine if reading incomplete index granules is supported.
    const IMergeTreeReader * main_reader;

    MergeTreeIndexWithCondition index;

    MarkRanges index_ranges;

    std::optional<MergeTreeIndexReader> index_reader;

    /// Current row position used when continuing reads across multiple calls.
    size_t current_row = 0;
    size_t current_mark = 0;
    std::map<size_t, Granule> granules;
};

}
