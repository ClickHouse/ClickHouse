#pragma once
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MergeTreeIndexReader.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostingListCursor.h>
#include <Storages/MergeTree/TextIndexPositionData.h>
#include <Storages/MergeTree/TextIndexPositionCodec.h>
#include <Storages/MergeTree/TextIndexBlockedPositionsCodec.h>
#include <Storages/MergeTree/TextIndexCache.h>
#include <Interpreters/ExpressionActions.h>

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <roaring/roaring.hh>

namespace DB
{

class TextIndexAnalyzer;
class MergeTreeIndexConditionText;

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
        size_t current_range_last_mark,
        bool continue_reading,
        size_t max_rows_to_read,
        MutableColumns & res_columns) override;

    /// The virtual columns are resolved from per-mark posting lists addressed
    /// by absolute row number, so a read may start or stop inside a mark.
    bool canReadIncompleteGranules() const override { return can_read_incomplete_granules; }
    void updateAllMarkRanges(const MarkRanges & ranges) override;
    void updateReadRequestMap(MarkRangesPtr request_map) override;

    /// Sets a pre-computed granule from the skip index reader (Path 2: use_skip_indexes_on_data_read = 1).
    /// Looks up its own index name in the map.
    void setPrecomputedGranule(const IndexGranulesMap & granules);

private:
    void setIndexGranule(MergeTreeIndexGranulePtr index_granule);
    void initializeFallbackReader(const IMergeTreeReader * main_reader);
    void createEmptyColumns(MutableColumns & columns, size_t max_rows_to_read) const;
    /// Opens the postings stream of one token, with the buffer sized to the token's largest segment.
    std::unique_ptr<MergeTreeReaderStream> makePostingsStream(const TokenPostingsInfo & token_info) const;
    /// The postings stream of a token, opened on first use and kept in `postings_streams`.
    MergeTreeReaderStream & getPostingsStream(std::string_view token, const TokenPostingsInfo & token_info);

    /// Returns combined postings per column for the given mark, clipped to `slice_range`
    /// (the actual read window, which may be narrower than the mark on partial-mark reads).
    std::vector<PostingList> buildPostingsForMark(size_t mark, const RowsRange & slice_range, PostingList & range_posting);
    /// Returns combined posting list for a single query by taking the prebuilt
    /// postings from the analyzer and reading large postings blocks as needed.
    PostingList buildPostingsForQuery(const TextSearchQuery & query, const TextIndexAnalyzer & analyzer, const RowsRange & range, PostingList & range_posting);
    /// Reads and unions all posting list blocks for a large-posting token within the given range.
    std::vector<PostingListPtr> readPostingsBlocksForToken(std::string_view token, const TokenPostingsInfo & token_info, const RowsRange & range);
    /// Removes blocks with max value less than the given range.
    void cleanupPostingsBlocks(const RowsRange & range);
    /// Drops all cached cursors, keeping the per-column sizing.
    void resetCursors();

    std::optional<RowsRange> getRowsRangeForMark(size_t mark) const;
    MergeTreeDataPartPtr getDataPart() const;

    void readGranule();
    /// Sets per-column flags from the analyzer's verdict and collects tokens to materialize.
    void classifyVirtualColumns();
    /// Collects the tokens whose postings the analysis left to read into `tokens_to_read`.
    void initializeTokensToRead();
    void fillColumn(IColumn & column, const PostingList & postings, size_t row_offset, size_t num_rows);
    void fillColumnLazy(IColumn & column, size_t column_idx, size_t row_offset, size_t num_rows, PostingList & range_posting);

    /// Search of one column resolved once per part, since none of it depends on the granule.
    /// Non-owning: the cursors belong to `lazy_cursors`, `direct_postings` to the analyzer.
    struct ResolvedSearch
    {
        enum class Kind : uint8_t
        {
            /// No row matches.
            Zeros,
            /// Only analyzer-folded postings: fill from `direct_postings` clipped to the granule.
            DirectPostings,
            /// Union (`Any`) or intersection (`All`) of `cursors`.
            Cursors,
        };

        Kind kind = Kind::Zeros;
        std::vector<PostingListCursor *> cursors;
        TextSearchMode mode = TextSearchMode::Any;
        TextIndexPostingsIntersectionAlgorithm intersection_algorithm = TextIndexPostingsIntersectionAlgorithm::Leapfrog;
        const PostingList * direct_postings = nullptr;
    };

    /// Also creates the cursors of the column in `lazy_cursors`.
    ResolvedSearch resolveSearch(size_t column_idx);

    /// Fills a virtual column for an abandoned pattern query by evaluating the virtual column's
    /// default expression (the original search predicate) on the physical columns.
    /// Used when the dictionary scan was cut short and pattern tokens are incomplete.
    void fillColumnFallback(
        IColumn & column,
        const String & column_name,
        const Block & physical_block,
        size_t offset,
        size_t num_rows) const;

    PostingListCursorPtr makeLazyCursor(std::string_view token, const TokenPostingsInfo & token_info);

    /// Fills a phrase virtual column from positional data (.pos), computing matching documents
    /// via phrase intersection (cached per granule).
    void applyPostingsPhrase(IColumn & column, const TextSearchQueryPtr & search_query, size_t row_offset, size_t num_rows);
    void initializePositionsStream();

    /// Intersects the phrase tokens' postings into candidates, then decodes only the covering blocks.
    PaddedPODArray<UInt32> phraseSearchBlocked(const TextSearchQuery & search_query);
    /// One token's full posting list — the rank space the blocked position stream is addressed in.
    PostingList readAllPostingsForToken(std::string_view token, const TokenPostingsInfo & token_info);

    using TextIndexGranulePtr = std::shared_ptr<const MergeTreeIndexGranuleText>;

    MergeTreeIndexWithCondition index;
    bool can_read_incomplete_granules = false;
    std::shared_ptr<MergeTreeIndexConditionText> condition_text;
    std::vector<TextSearchQueryPtr> search_queries;
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
    /// A separate stream is created for each token to read postings blocks continuously without additional seeks.
    absl::flat_hash_map<std::string_view, std::unique_ptr<MergeTreeReaderStream>> postings_streams;
    /// Tokens the analysis left to read: needed by some query and without postings read during the analysis.
    absl::flat_hash_set<std::string_view> tokens_to_read;

    /// Stream for position data (.pos file) used for phrase queries.
    std::unique_ptr<MergeTreeReaderStream> positions_stream;
    /// Per-reader memo of phrase results (shared via the postings cache) so repeated readRows calls skip the cache lookup.
    absl::flat_hash_map<UInt128, FlatPostingsPtr> phrase_search_doc_ids;

    /// Current row position used when continuing reads across multiple calls.
    size_t current_row = 0;
    size_t current_mark = 0;
    PaddedPODArray<UInt32> indices_buffer;
    TextIndexBlockedPositionsCodec::DecodeScratch blocked_positions_scratch;

    bool is_initialized = false;
    /// Virtual columns that are always true.
    std::vector<bool> is_always_true;
    std::unique_ptr<MergeTreeIndexDeserializationState> deserialization_state;
    std::optional<PostingsSerialization> postings_serialization;

    /// Requested in the constructor; enabled per granule in `setIndexGranule` after checking the
    /// sparse-index header and confirming no virtual column carries pattern predicates.
    bool lazy_mode_requested = false;
    bool use_lazy_mode = false;
    TextIndexPostingsIntersectionAlgorithm intersection_algorithm = TextIndexPostingsIntersectionAlgorithm::Auto;

    /// Owns the cursors of `resolved_searches`. Cursors are forward-only and hold mutable segment/block
    /// position, so each column gets its own. Dropped on granule reload and on backward `readRows` jumps.
    std::vector<PostingListCursorPtr> lazy_cursors;

    /// Per-column `ResolvedSearch`, built on the first granule. Dropped together with `lazy_cursors`.
    std::vector<std::optional<ResolvedSearch>> resolved_searches;

    /// Counters of the lazy intersections, added to the profile events when the reader is destroyed.
    LazyPostingsStats lazy_postings_stats;
};

MergeTreeReaderPtr createMergeTreeReaderTextIndex(
    const IMergeTreeReader * main_reader,
    const MergeTreeIndexWithCondition & index,
    const NamesAndTypesList & columns_to_read,
    MergeTreeIndexGranulePtr index_granule);

}
