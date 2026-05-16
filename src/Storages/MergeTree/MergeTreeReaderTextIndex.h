#pragma once
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeIndexReader.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/TextIndexCache.h>
#include <Interpreters/ExpressionActions.h>

#include <absl/container/flat_hash_map.h>
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
        MergeTreeIndexGranulePtr index_granule_);

    size_t readRows(
        size_t from_mark,
        size_t current_task_last_mark,
        bool continue_reading,
        size_t max_rows_to_read,
        size_t offset,
        Columns & res_columns) override;

    bool canReadIncompleteGranules() const override { return false; }
    void updateAllMarkRanges(const MarkRanges & ranges) override;

    /// Sets a pre-computed granule from the skip index reader (Path 2: use_skip_indexes_on_data_read = 1).
    /// Looks up its own index name in the map.
    void setPrecomputedGranule(const IndexGranulesMap & granules);

private:
    void initializeFallbackReader(const IMergeTreeReader * main_reader);
    void createEmptyColumns(Columns & columns) const;

    /// Returns postings for all all tokens required for the given mark.
    PostingsMap readPostingsIfNeeded(size_t mark);
    /// Returns postings for all blocks of the given token required for the given range.
    std::vector<PostingListPtr> readPostingsBlocksForToken(std::string_view token, const TokenPostingsInfo & token_info, const RowsRange & range);
    /// Removes blocks with max value less than the given range.
    void cleanupPostingsBlocks(const RowsRange & range);

    /// Fills a virtual column for an abandoned pattern query by evaluating the virtual column's
    /// default expression (the original search predicate) on the physical columns.
    /// Used when the dictionary scan was cut short and pattern tokens are incomplete.
    void fillColumnFallback(
        IColumn & column,
        const String & column_name,
        const Block & physical_block,
        size_t offset,
        size_t num_rows) const;

    std::optional<RowsRange> getRowsRangeForMark(size_t mark) const;
    MergeTreeDataPartPtr getDataPart() const;

    void readGranule();
    void analyzeTokensCardinality();
    void initializePostingStreams();
    void fillColumn(IColumn & column, const String & column_name, PostingsMap & postings, size_t row_offset, size_t num_rows);
    double estimateCardinality(const TextSearchQuery & query, const TokenToPostingsInfosMap & remaining_tokens, size_t total_rows) const;

    using TextIndexGranulePtr = std::shared_ptr<const MergeTreeIndexGranuleText>;

    MergeTreeIndexWithCondition index;
    TextIndexGranulePtr granule;
    PostingsBlocksMap postings_blocks;

    /// Fallback reader for the physical columns required by the fallback expressions.
    /// Used when the pattern dictionary scan is cut short.
    MergeTreeReaderPtr fallback_reader;
    /// Physical columns that fallback_reader reads (union across all fallback expressions).
    NamesAndTypesList fallback_columns_list;
    /// Per-virtual-column compiled expression of the original search predicate.
    /// Executed on the physical columns when use_fallback[i] is true.
    absl::flat_hash_map<String, ExpressionActionsPtr> fallback_expressions;
    /// Per-virtual-column flag: true if this column's query was abandoned during the scan
    /// and the predicate must be evaluated directly via fallback_expressions.
    std::vector<bool> use_fallback;
    /// A separate stream is created for each token to read
    /// postings blocks continuously without additional seeks.
    absl::flat_hash_map<std::string_view, std::unique_ptr<MergeTreeReaderStream>> large_postings_streams;

    /// Current row position used when continuing reads across multiple calls.
    size_t current_row = 0;
    size_t current_mark = 0;
    PaddedPODArray<UInt32> indices_buffer;

    bool is_initialized = false;
    /// Virtual columns that are always true.
    std::vector<bool> is_always_true;
    /// Tokens that are useful for analysis and filling virtual columns.
    absl::flat_hash_set<std::string_view> useful_tokens;
    std::unique_ptr<MergeTreeIndexDeserializationState> deserialization_state;
    PostingsSerialization postings_serialization;
};

}
