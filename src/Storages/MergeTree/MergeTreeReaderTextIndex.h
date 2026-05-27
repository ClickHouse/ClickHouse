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

class TextIndexAnalyzer;

class PostingListCursor;
using PostingListCursorPtr = std::shared_ptr<PostingListCursor>;

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
    void setIndexGranule(MergeTreeIndexGranulePtr index_granule);
    void initializeFallbackReader(const IMergeTreeReader * main_reader);
    void createEmptyColumns(Columns & columns) const;
    std::unique_ptr<MergeTreeReaderStream> makeTextIndexStream(const MergeTreeIndexSubstream & substream) const;

    /// Returns combined postings per column for the given mark, clipped to `slice_range`
    /// (the actual read window, which may be narrower than the mark on partial-mark reads).
    std::vector<PostingList> buildPostingsForMark(size_t mark, const RowsRange & slice_range);
    /// Returns combined posting list for a single query by taking the prebuilt
    /// postings from the analyzer and reading large postings blocks as needed.
    PostingList buildPostingsForQuery(const TextSearchQuery & query, const TextIndexAnalyzer & analyzer, const RowsRange & range);
    /// Reads and unions all posting list blocks for a large-posting token within the given range.
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
    /// Sets per-column flags from the analyzer's verdict and collects tokens to materialize.
    void classifyVirtualColumns();
    void initializePostingStreams();
    void fillColumn(IColumn & column, const PostingList & postings, size_t row_offset, size_t num_rows);
    void fillColumnLazy(IColumn & column, const String & column_name, size_t row_offset, size_t num_rows);
    PostingListCursorPtr makeLazyCursor(std::string_view token, const TokenPostingsInfo & token_info);

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
    /// Small postings stream — kept as a class member because cached lazy cursors
    /// hold a reference to it for on-demand segment reads.
    std::unique_ptr<MergeTreeReaderStream> small_postings_stream;
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
    std::unique_ptr<MergeTreeIndexDeserializationState> deserialization_state;
    std::optional<PostingsSerialization> postings_serialization;

    /// Requested in the constructor; enabled per granule in `setIndexGranule` after checking the
    /// sparse-index header and confirming no virtual column carries pattern predicates.
    bool lazy_mode_requested = false;
    bool use_lazy_mode = false;
    float lazy_density_threshold = 0.2f;

    /// Cached lazy cursors keyed by `(virtual column name, token)`. Cursors are forward-only and
    /// hold mutable segment/block position, so they must not be shared across columns.
    /// Cleared on granule reload and on backward `readRows` jumps (`from_mark < current_mark`).
    absl::flat_hash_map<String, absl::flat_hash_map<String, PostingListCursorPtr>> lazy_cursors;

    /// Per-column synthetic cursor over the analyzer-folded postings of small/embedded tokens,
    /// combined with large-posting stream cursors. Cleared on the same triggers as `lazy_cursors`.
    absl::flat_hash_map<String, PostingListCursorPtr> prebuilt_cursors;
};

}
