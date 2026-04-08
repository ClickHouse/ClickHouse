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
        std::optional<PostingList> postings;
        std::optional<RowsRange> rows_range;

        bool is_failed = false;
        bool is_bypassed = false;
        bool has_large_postings = false;

        void markFailed();
        void markBypassed();
        void addMissingToken();
        void addLargePostings() { has_large_postings = true; }
        void addRowsRange(RowsRange token_rows_range);
        void addPostings(PostingListPtr token_postings);
    };

    explicit TextIndexAnalyzer(const MergeTreeIndexConditionText & condition_text);

    bool alwaysFalse() const { return always_false; }
    const TokenToPostingsInfosMap & getTokenInfos() const { return token_infos; }
    const NameSet & getMissingTokens() const { return missing_tokens; }
    const QueryBuilder & getQueryBuilder(const TextSearchQuery & query) const;

    bool hasReadPostings(std::string_view token);
    bool isTokenNeeded(std::string_view token) const;

    void addMissingToken(std::string_view token);
    void addLargePostings(std::string_view token);
    void addTokenInfo(std::string_view token, TokenPostingsInfoPtr token_info);
    void addPostings(std::string_view token, PostingListPtr postings);

    bool addTokenToPatterns(std::string_view token);
    bool isBypassed(const TextSearchQuery & query) const;
    void bypassPatternQueries();

private:
    template <typename Operation>
    void processTokenOperation(std::string_view token, Operation && operation);
    void decrementQueryCount(const TextSearchQuery & query);

    TextSearchMode global_search_mode;
    absl::flat_hash_map<UInt128, QueryBuilder> query_builders;
    absl::flat_hash_map<String, size_t> query_count_by_token;
    absl::flat_hash_map<String, std::vector<UInt128>> queries_by_token;
    absl::flat_hash_map<const OptimizedRegularExpression *, std::vector<UInt128>> queries_by_pattern;

    bool always_false = false;
    NameSet missing_tokens;
    TokenToPostingsInfosMap token_infos;
    TokenToPostingsMap small_postings;
};

}
