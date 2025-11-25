#pragma once
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeIndexReader.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/TextIndexCache.h>
#include <roaring.hh>

namespace DB
{

using PostingsMap = absl::flat_hash_map<StringRef, PostingListPtr>;

/// A part of "direct read from text index" optimization.
/// This reader fills virtual columns for text search filters
/// which were replaced from the text search functions using
/// the posting lists read from the index.
///
/// E.g. `__text_index_<name>_hasToken` column created for `hasToken` function.
class MergeTreeReaderTextIndex : public IMergeTreeReader
{
public:
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
    void prefetchBeginOfRange(Priority priority) override;

private:
    struct Granule
    {
        MergeTreeIndexGranulePtr granule;
        PostingsMap postings;
        bool may_be_true = true;
        bool need_read_postings = true;
    };

    void updateAllIndexRanges();
    void createEmptyColumns(Columns & columns) const;
    void readPostingsIfNeeded(Granule & granule, size_t index_mark);
    void fillSkippedColumn(IColumn & column, size_t num_rows);
    void fillColumn(IColumn & column, Granule & granule, const String & column_name, size_t granule_offset, size_t num_rows);

    MergeTreeIndexWithCondition index;
    MarkRanges all_index_ranges;
    std::optional<MergeTreeIndexReader> index_reader;

    /// Current row position used when continuing reads across multiple calls.
    size_t current_row = 0;
    size_t current_mark = 0;

    /// Counts marks remaining to read in index granule.
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
    PaddedPODArray<UInt32> indices_buffer;
    roaring::Roaring analyzed_granules;
};

}
