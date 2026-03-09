#pragma once
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeIndexReader.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/TextIndexCache.h>
#include <absl/container/flat_hash_set.h>
#include <roaring/roaring.hh>

namespace DB
{

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
        MergeTreeIndexGranulePtr granule_);

    void setGranule(MergeTreeIndexGranulePtr granule_);
    const String & getIndexName() const;

    size_t readRows(
        size_t from_mark,
        size_t current_task_last_mark,
        bool continue_reading,
        size_t max_rows_to_read,
        size_t offset,
        Columns & res_columns) override;

    bool canReadIncompleteGranules() const override { return false; }
    void updateAllMarkRanges(const MarkRanges & ranges) override;

private:
    void createEmptyColumns(Columns & columns) const;

    /// Returns combined postings per column for the given mark.
    std::vector<PostingList> buildPostingsForMark(size_t mark);
    /// Returns combined posting list for a single query by taking the prebuilt
    /// postings from the analyzer and reading large postings blocks as needed.
    PostingList buildPostingsForQuery(const TextSearchQuery & query, const TextIndexAnalyzer & analyzer, const RowsRange & range);
    /// Reads and unions all posting list blocks for a large-posting token within the given range.
    std::vector<PostingListPtr> readPostingsBlocksForToken(std::string_view token, const TokenPostingsInfo & token_info, const RowsRange & range);
    /// Removes blocks with max value less than the given range.
    void cleanupPostingsBlocks(const RowsRange & range);

    std::optional<RowsRange> getRowsRangeForMark(size_t mark) const;
    MergeTreeDataPartPtr getDataPart() const;

    void analyzeTokensCardinality();
    void initializePostingStreams();
    void fillColumn(IColumn & column, const PostingList & postings, size_t row_offset, size_t num_rows);

    size_t getNumRowsInGranule(size_t index_mark) const;
    double estimateCardinality(const TextSearchQuery & query, const TokenToPostingsInfosMap & remaining_tokens, size_t total_rows) const;

    MergeTreeIndexWithCondition index;
    MergeTreeIndexGranulePtr granule;
    PostingsBlocksMap postings_blocks;

    bool is_initialized = false;

    /// Streams for large postings that are split into multiple blocks.
    /// A separate stream is created for each token to read
    /// postings blocks continuously without additional seeks.
    absl::flat_hash_map<std::string_view, std::unique_ptr<MergeTreeReaderStream>> large_postings_streams;

    /// Current row position used when continuing reads across multiple calls.
    size_t current_row = 0;
    size_t current_mark = 0;

    PaddedPODArray<UInt32> indices_buffer;

    /// Virtual columns that are always true.
    std::vector<bool> is_always_true;
    /// Tokens that are useful for analysis and filling virtual columns.
    absl::flat_hash_set<std::string_view> useful_tokens;
    std::unique_ptr<MergeTreeIndexDeserializationState> deserialization_state;
    PostingsSerialization postings_serialization;
    String index_id_for_caches;
};

}
