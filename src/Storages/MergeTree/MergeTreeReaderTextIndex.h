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

    /// Returns postings for all all tokens required for the given mark.
    PostingsMap readPostingsIfNeeded(size_t mark);
    /// Returns postings for all blocks of the given token required for the given range.
    std::vector<PostingListPtr> readPostingsBlocksForToken(std::string_view token, const TokenPostingsInfo & token_info, const RowsRange & range);
    /// Removes blocks with max value less than the given range.
    void cleanupPostingsBlocks(const RowsRange & range);

    std::optional<RowsRange> getRowsRangeForMark(size_t mark) const;
    MergeTreeDataPartPtr getDataPart() const;

    void readGranule();
    void analyzeTokensCardinality();
    void initializePostingStreams();
    void fillColumn(IColumn & column, const String & column_name, PostingsMap & postings, size_t row_offset, size_t num_rows);

    using TokenToPostingsInfosMap = MergeTreeIndexGranuleText::TokenToPostingsInfosMap;

    size_t getNumRowsInGranule(size_t index_mark) const;
    double estimateCardinality(const TextSearchQuery & query, const TokenToPostingsInfosMap & remaining_tokens, size_t total_rows) const;

    MergeTreeIndexWithCondition index;
    MergeTreeIndexGranulePtr granule;
    PostingsBlocksMap postings_blocks;

    /// True if the reader is allowed to skip marks.
    /// Otherwise it only fills virtual columns.
    bool can_skip_mark;
    bool is_prefetched = false;

    std::unique_ptr<MergeTreeReaderStream> sparse_index_stream;
    std::unique_ptr<MergeTreeReaderStream> dictionary_stream;

    /// Stream for small postings that are embedded or has one block.
    std::unique_ptr<MergeTreeReaderStream> small_postings_stream;
    /// Streams for large postings that are split into multiple blocks.
    /// A separate stream is created for each token to read
    /// postings blocks continuously without additional seeks.
    absl::flat_hash_map<std::string_view, std::unique_ptr<MergeTreeReaderStream>> large_postings_streams;

    /// Current row position used when continuing reads across multiple calls.
    size_t current_row = 0;
    size_t current_mark = 0;

    PaddedPODArray<UInt32> indices_buffer;
    roaring::Roaring analyzed_granules;
    roaring::Roaring may_be_true_granules;

    /// Virtual columns that are always true.
    std::vector<bool> is_always_true;
    /// Tokens that are useful for analysis and filling virtual columns.
    absl::flat_hash_set<std::string_view> useful_tokens;
    std::unique_ptr<MergeTreeIndexDeserializationState> deserialization_state;
};

}
