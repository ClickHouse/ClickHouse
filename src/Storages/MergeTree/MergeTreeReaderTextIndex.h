#pragma once
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeIndexReader.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>

namespace DB
{

using PostingsMap = absl::flat_hash_map<StringRef, CompressedPostings::Iterator>;

class MergeTreeReaderTextIndex : public IMergeTreeReader
{
public:
    using MatchingMarks = std::vector<bool>;

    MergeTreeReaderTextIndex(
        const IMergeTreeReader * main_reader_,
        MergeTreeIndexWithCondition index_,
        NamesAndTypesList columns_);

    size_t readRows(
        size_t from_mark,
        size_t current_task_last_mark,
        bool continue_reading,
        size_t max_rows_to_read,
        size_t offset,
        Columns & res_columns) override;

    bool canSkipMark(size_t mark, size_t current_task_last_mark) override;
    bool canReadIncompleteGranules() const override { return false; }
    void updateAllMarkRanges(const MarkRanges & ranges) override;

private:
    struct Granule
    {
        MergeTreeIndexGranulePtr granule;
        PostingsMap postings;
        std::list<CompressedPostings> postings_holders;
        bool may_be_true = true;
        bool need_read_postings = true;
    };

    void updateAllIndexRanges();
    void createEmptyColumns(Columns & columns) const;
    void readPostingsIfNeeded(Granule & granule);
    void fillSkippedColumn(IColumn & column, size_t num_rows);
    void fillColumn(IColumn & column, Granule & granule, const String & column_name, size_t granule_offset, size_t num_rows) const;

    MergeTreeIndexWithCondition index;
    MarkRanges all_index_ranges;
    std::optional<MergeTreeIndexReader> index_reader;

    /// Current row position used when continuing reads across multiple calls.
    size_t current_row = 0;
    size_t current_mark = 0;

    struct RemainingMarks
    {
        size_t total = 0;
        size_t remaining = 0;

        void increment();
        /// Returns true if granule can be removed.
        bool decrement(size_t granularity);
        bool finished(size_t granularity) const;
    };

    std::map<size_t, Granule> granules;
    absl::flat_hash_map<size_t, RemainingMarks> remaining_marks;
};

}
