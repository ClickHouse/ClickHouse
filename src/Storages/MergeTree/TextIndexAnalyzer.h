#pragma once

#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeIndexConditionText.h>

#include <absl/container/flat_hash_map.h>

namespace DB
{

class TextIndexAnalyzer
{
public:
    struct QueryBuilder
    {
        TextSearchQueryPtr query;
        TokenToPostingsInfosMap tokens;

        std::optional<PostingList> postings;
        std::optional<RowsRange> rows_range;

        bool is_failed = false;
        bool is_bypassed = false;
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
    const TokenToPostingsInfosMap & getTokenInfos() const { return token_infos; }
    const absl::flat_hash_set<String> & getMissingTokens() const { return missing_tokens; }
    const QueryBuilder & getQueryBuilder(const TextSearchQuery & query) const;

    bool isTokenNeeded(std::string_view token) const;
    bool hasReadPostings(std::string_view token) const { return tokens_with_postings.contains(token); }

    void addMissingToken(std::string_view token);
    void addTokenInfo(std::string_view token, TokenPostingsInfoPtr token_info);
    void addPostings(std::string_view token, PostingListPtr postings);

    bool addTokenToPatterns(std::string_view token);
    void bypassPatternQueries();

    /// For each `Hint`-mode query that is still active, estimate the resulting cardinality
    /// from already-read postings combined with `cardinality` estimates for tokens whose
    /// posting lists are still unread (multi-block tokens). Queries whose estimate exceeds
    /// `selectivity_threshold * total_rows` are marked discarded.
    void analyzeCardinalitiesAndBypassHints(double selectivity_threshold, size_t total_rows);

    size_t memoryUsageBytes() const;

private:
    using QueryHashes = absl::flat_hash_set<UInt128>;

    template <typename Operation>
    void processTokenOperation(std::string_view token, Operation && operation);

    double estimateQueryCardinality(const QueryBuilder & query_builder, size_t total_rows) const;

    TextSearchMode global_search_mode;
    bool always_false = false;

    absl::flat_hash_map<UInt128, QueryBuilder> query_builders;
    absl::flat_hash_map<String, QueryHashes> queries_by_token;
    absl::flat_hash_map<const OptimizedRegularExpression *, QueryHashes> queries_by_pattern;

    TokenToPostingsInfosMap token_infos;
    absl::flat_hash_set<String> missing_tokens;
    absl::flat_hash_set<String> tokens_with_postings;
};

}
