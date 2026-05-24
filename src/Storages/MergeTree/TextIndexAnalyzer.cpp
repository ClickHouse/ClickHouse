#include <Storages/MergeTree/TextIndexAnalyzer.h>

#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void TextIndexAnalyzer::QueryBuilder::markFailed()
{
    is_failed = true;
    rows_range.reset();
    postings.reset();
}

void TextIndexAnalyzer::QueryBuilder::markBypassed()
{
    is_bypassed = true;
    rows_range.reset();
    postings.reset();
}

void TextIndexAnalyzer::QueryBuilder::addMissingToken()
{
    if (query->search_mode == TextSearchMode::All)
        markFailed();
}

void TextIndexAnalyzer::QueryBuilder::addTokenInfo(std::string_view token, TokenPostingsInfoPtr token_info)
{
    if (is_failed)
        return;

    tokens[token] = token_info;

    chassert(!token_info->ranges.empty());
    RowsRange token_rows_range(token_info->ranges.front().begin, token_info->ranges.back().end);
    addRowsRange(token_rows_range);

    if (token_info->embedded_postings)
        addPostings(token_info->embedded_postings);
}

void TextIndexAnalyzer::QueryBuilder::addRowsRange(RowsRange token_rows_range)
{
    if (is_failed)
        return;

    if (!rows_range)
    {
        rows_range = token_rows_range;
    }
    else if (query->search_mode == TextSearchMode::Any)
    {
        rows_range = rows_range->unionWith(token_rows_range);
    }
    else if (query->search_mode == TextSearchMode::All)
    {
        rows_range = rows_range->intersectWith(token_rows_range);

        if (!rows_range)
            markFailed();
    }
}

void TextIndexAnalyzer::QueryBuilder::addPostings(PostingListPtr token_postings)
{
    if (is_failed)
        return;

    ++num_read_postings;

    if (!postings)
    {
        postings = *token_postings;
    }
    else if (query->search_mode == TextSearchMode::Any)
    {
        *postings |= *token_postings;
    }
    else
    {
        *postings &= *token_postings;

        if (postings->cardinality() == 0)
            markFailed();
    }
}

TextIndexAnalyzer::TextIndexAnalyzer(const MergeTreeIndexConditionText & condition_text)
{
    global_search_mode = condition_text.getGlobalSearchMode();

    for (const auto & [hash, query] : condition_text.getAllSearchQueries())
    {
        query_builders[hash].query = query;

        for (const auto & token : query->tokens)
            queries_by_token[token].insert(hash);

        for (const auto & pattern : query->patterns)
            queries_by_pattern[&pattern].insert(hash);
    }
}

const TextIndexAnalyzer::QueryBuilder & TextIndexAnalyzer::getQueryBuilder(const TextSearchQuery & query) const
{
    auto hash = query.getHash().get128();
    auto it = query_builders.find(hash);

    if (it == query_builders.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query builder not found for text search query with function '{}'", query.function_name);

    return it->second;
}

void TextIndexAnalyzer::addMissingToken(std::string_view token)
{
    missing_tokens.emplace(token);

    processTokenOperation(token, [&](QueryBuilder & query_builder)
    {
        query_builder.addMissingToken();
    });
}

void TextIndexAnalyzer::addTokenInfo(std::string_view token, TokenPostingsInfoPtr token_info)
{
    token_infos[token] = token_info;
    if (token_info->embedded_postings)
        tokens_with_postings.emplace(token);

    processTokenOperation(token, [&](QueryBuilder & query_builder)
    {
        query_builder.addTokenInfo(token, token_info);
    });
}

void TextIndexAnalyzer::addPostings(std::string_view token, PostingListPtr postings)
{
    tokens_with_postings.emplace(token);

    processTokenOperation(token, [&](QueryBuilder & query_builder)
    {
        query_builder.addPostings(postings);
    });
}

bool TextIndexAnalyzer::addTokenToPatterns(std::string_view token)
{
    bool added = false;

    for (const auto & [pattern, query_hashes] : queries_by_pattern)
    {
        if (pattern->match(token.data(), token.size()))
        {
            added = true;

            for (const auto & query_hash : query_hashes)
                queries_by_token[token].emplace(query_hash);
        }
    }

    return added;
}

bool TextIndexAnalyzer::isTokenNeeded(std::string_view token) const
{
    auto it = queries_by_token.find(token);
    return it != queries_by_token.end() && !it->second.empty();
}

void TextIndexAnalyzer::bypassPatternQueries()
{
    QueryHashes all_pattern_queries;
    for (const auto & [_, query_hashes] : queries_by_pattern)
    {
        for (const auto & query_hash : query_hashes)
            all_pattern_queries.insert(query_hash);
    }

    for (const auto & query_hash : all_pattern_queries)
    {
        auto & query_builder = query_builders[query_hash];
        query_builder.markBypassed();

        for (const auto & [query_token, _] : query_builder.tokens)
            queries_by_token[query_token].erase(query_hash);
    }
}

template <typename Operation>
void TextIndexAnalyzer::processTokenOperation(std::string_view token, Operation && operation)
{
    /// Copy the set of query hashes before iterating, because
    /// erasing a failed query from queries_by_token below may
    /// mutate this very set (when query_token == token).
    auto token_queries = queries_by_token.at(token);

    for (const auto & query_hash : token_queries)
    {
        auto & query_builder = query_builders[query_hash];
        if (query_builder.is_failed || query_builder.is_bypassed)
            continue;

        operation(query_builder);

        if (query_builder.is_failed)
        {
            if (global_search_mode == TextSearchMode::All)
                always_false = true;

            for (const auto & [query_token, _] : query_builder.tokens)
                queries_by_token[query_token].erase(query_hash);
        }
    }
}

/// Estimate memory footprint of an absl::flat_hash_map/set.
/// absl flat containers use open addressing with one control byte per slot.
template <typename Container>
static size_t estimateAbslFlatContainerBytes(const Container & c)
{
    return c.empty() ? 0 : c.capacity() * (sizeof(typename Container::value_type) + 1);
}

size_t TextIndexAnalyzer::memoryUsageBytes() const
{
    size_t result = sizeof(*this);

    /// query_builders: map<UInt128, QueryBuilder>, each QueryBuilder has tokens map and optional postings.
    result += estimateAbslFlatContainerBytes(query_builders);
    for (const auto & [_, query_builder] : query_builders)
    {
        result += estimateAbslFlatContainerBytes(query_builder.tokens);
        if (query_builder.postings)
            result += query_builder.postings->getSizeInBytes();
    }

    /// queries_by_token: map<String, QueryHashes>.
    result += estimateAbslFlatContainerBytes(queries_by_token);
    for (const auto & [key, hashes] : queries_by_token)
    {
        result += key.capacity();
        result += estimateAbslFlatContainerBytes(hashes);
    }

    /// queries_by_pattern: map<ptr, QueryHashes>.
    result += estimateAbslFlatContainerBytes(queries_by_pattern);
    for (const auto & [_, hashes] : queries_by_pattern)
        result += estimateAbslFlatContainerBytes(hashes);

    /// token_infos: map<String, TokenPostingsInfoPtr>.
    result += estimateAbslFlatContainerBytes(token_infos);
    for (const auto & [key, _] : token_infos)
        result += key.capacity();

    /// missing_tokens: set<String>.
    result += estimateAbslFlatContainerBytes(missing_tokens);
    for (const auto & token : missing_tokens)
        result += token.capacity();

    /// tokens_with_postings: set<String>.
    result += estimateAbslFlatContainerBytes(tokens_with_postings);
    for (const auto & token : tokens_with_postings)
        result += token.capacity();

    return result;
}

}
