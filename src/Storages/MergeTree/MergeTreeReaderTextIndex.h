#pragma once
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeIndexReader.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/TextIndexCache.h>
#include <roaring.hh>
#include <absl/container/flat_hash_set.h>

namespace DB
{

using PostingsMap = absl::flat_hash_map<std::string_view, PostingListPtr>;
using PostingsBlocksMap = absl::flat_hash_map<std::string_view, absl::btree_map<size_t, PostingListPtr>>;

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
        NamesAndTypesList columns_,
        bool can_skip_mark_);

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
    void createEmptyColumns(Columns & columns) const;

    PostingsMap readPostingsIfNeeded(size_t mark);
    std::vector<PostingListPtr> readPostingsForToken(std::string_view token, const TokenPostingsInfo & token_info, const RowsRange & range);
    PostingListPtr readPostingsBlock(std::string_view token, size_t offset_in_file);
    void cleanupPostingsBlocks(const RowsRange & range);

    std::optional<RowsRange> getRowsRangeForMark(size_t mark) const;
    MergeTreeDataPartPtr getDataPart() const;

    void readGranule();
    void analyzeTokensCardinality();
    void initializePostingStreams();

    void fillSkippedColumn(IColumn & column, size_t num_rows);
    void fillColumn(IColumn & column, const String & column_name, PostingsMap & postings, size_t row_offset, size_t num_rows);

    using TokenToPostingsInfosMap = MergeTreeIndexGranuleText::TokenToPostingsInfosMap;
    size_t getNumRowsInGranule(size_t index_mark) const;
    double estimateCardinality(const TextSearchQuery & query, const TokenToPostingsInfosMap & remaining_tokens, size_t total_rows) const;

    MergeTreeIndexWithCondition index;
    bool can_skip_mark;
    MergeTreeIndexGranulePtr granule;
    PostingsBlocksMap postings_blocks;

    std::unique_ptr<MergeTreeReaderStream> main_stream;
    std::unique_ptr<MergeTreeReaderStream> dictionary_stream;

    absl::flat_hash_map<std::string_view, MergeTreeReaderStream *> posting_streams;
    std::vector<std::unique_ptr<MergeTreeReaderStream>> posting_stream_holders;

    /// Current row position used when continuing reads across multiple calls.
    size_t current_row = 0;
    size_t current_mark = 0;

    PaddedPODArray<UInt32> indices_buffer;
    roaring::Roaring analyzed_granules;
    roaring::Roaring may_be_true_granules;

    absl::flat_hash_set<std::string_view> useful_tokens;
    std::vector<bool> is_always_true;
};

}
