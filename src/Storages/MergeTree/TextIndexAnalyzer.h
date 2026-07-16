#pragma once
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>
#include <absl/container/flat_hash_map.h>

namespace DB
{

/// Drives text-index analysis during a granule's dictionary scan: folds per-query
/// token postings and row ranges, then bypasses queries that have failed or are no
/// longer worth evaluating (low-selectivity hints, pattern bypass).
class TextIndexAnalyzer
{
public:
    /// Per-query mutable analysis state. Updated as the dictionary scan delivers
    /// token info, missing-token notifications, and materialized posting lists.
    struct QueryBuilder
    {
        /// Original parsed search query (tokens + patterns + search mode).
        TextSearchQueryPtr query;
        /// Tokens this query has observed so far (declared + pattern-discovered).
        TokenToPostingsInfosMap tokens;
        /// Row range folded across observed tokens by `query->search_mode` (intersect for `All`, union for `Any`).
        std::optional<RowsRange> rows_range;
        /// Posting list folded across materialized tokens by `query->search_mode`.
        std::optional<PostingList> postings;

        /// Query can never match (e.g. missing token in `All` mode, empty intersection).
        bool is_failed = false;
        /// Query was discarded (low-selectivity hint, pattern bypass).
        bool is_bypassed = false;
        /// Number of tokens whose posting list has already been folded into `postings`.
        size_t num_read_postings = 0;

        void markFailed();
        void markBypassed();
        void addMissingToken();
        void addTokenInfo(std::string_view token, TokenPostingsInfoPtr token_info);
        void addRowsRange(RowsRange token_rows_range);
        void addPostings(PostingListPtr token_postings);
        bool needReadPostings() const { return num_read_postings < tokens.size(); }
    };

    explicit TextIndexAnalyzer(const MergeTreeIndexConditionText & condition_text);

    bool alwaysFalse() const { return always_false; }
    const TokenToPostingsInfosMap & getAllTokenInfos() const { return all_token_infos; }
    const absl::flat_hash_set<String> & getMissingTokens() const { return missing_tokens; }
    const QueryBuilder & getQueryBuilder(const TextSearchQuery & query) const;

    /// True if at least one active query still depends on this token.
    bool isTokenNeeded(std::string_view token) const;
    /// True if this token's posting list has already been added (embdded or read from disk).
    bool hasReadPostings(std::string_view token) const;

    void addMissingToken(std::string_view token);
    void addTokenInfo(std::string_view token, TokenPostingsInfoPtr token_info);
    void addPostings(std::string_view token, PostingListPtr postings);

    /// Attaches a scan-discovered `token` to every pattern query whose regex matches it.
    /// Returns true if any pattern matched.
    bool addTokenToPatterns(std::string_view token);
    /// Marks all pattern queries as bypassed (e.g. dictionary scan budget exhausted).
    void bypassPatternQueries();

    /// Discards `Hint`-mode queries whose estimated cardinality (read postings + `cardinality`
    /// estimates for unread multi-block tokens) exceeds `selectivity_threshold * total_rows`.
    void analyzeCardinalitiesAndBypassHints(double selectivity_threshold, size_t total_rows);
    size_t memoryUsageBytes() const;

private:
    using QueryHashes = absl::flat_hash_set<UInt128>;

    /// Applies `operation` to every active query that references `token`,
    /// then cleans up `queries_by_token` for any query that just failed.
    template <typename Operation>
    void processTokenOperation(std::string_view token, Operation && operation);

    /// Estimates the cardinality of a query from already-read postings and `cardinality` hints for unread tokens.
    double estimateQueryCardinality(const QueryBuilder & query_builder, size_t total_rows) const;

    /* Fields built in the constructor from MergeTreeIndexConditionText. */

    TextSearchMode global_search_mode;
    /// One builder per parsed query, keyed by the query's stable hash.
    absl::flat_hash_map<UInt128, QueryBuilder> query_builders;
    /// Active queries that still depend on a given token.
    absl::flat_hash_map<String, QueryHashes> queries_by_token;
    /// Pattern queries grouped by their compiled regex; static for the analyzer's lifetime.
    absl::flat_hash_map<const OptimizedRegularExpression *, QueryHashes> queries_by_pattern;

    /* Fields updated dynamically during text index analysis. */

    bool always_false = false;
    /// Dictionary entries observed during the scan, keyed by token.
    TokenToPostingsInfosMap all_token_infos;
    /// Tokens looked up and not present in the dictionary.
    absl::flat_hash_set<String> missing_tokens;
    /// Tokens whose posting list has been added (embedded or read from disk).
    absl::flat_hash_set<String> tokens_with_postings;
};

}
