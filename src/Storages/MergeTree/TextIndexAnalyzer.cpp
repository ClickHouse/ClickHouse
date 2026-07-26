#include <Storages/MergeTree/TextIndexAnalyzer.h>
#include <Common/ProfileEvents.h>
#include <Common/typeid_cast.h>
#include <algorithm>
#include <cmath>

namespace ProfileEvents
{
    extern const Event TextIndexUseHint;
    extern const Event TextIndexDiscardHint;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


TextIndexAnalyzer::ReadableRows::ReadableRows(std::vector<RowsRange> ranges_)
    : ranges(std::move(ranges_))
{
}

std::optional<RowsRange> TextIndexAnalyzer::ReadableRows::clipRowsRange(const RowsRange & rows_range) const
{
    /// First readable range whose end reaches the span begin; ranges before it cannot overlap.
    auto it = std::lower_bound(
        ranges.begin(), ranges.end(), rows_range.begin,
        [](const RowsRange & range, size_t value) { return range.end < value; });

    std::optional<RowsRange> clipped;
    for (; it != ranges.end() && it->begin <= rows_range.end; ++it)
    {
        size_t begin = std::max(rows_range.begin, it->begin);
        size_t end = std::min(rows_range.end, it->end);

        if (begin > end)
            continue;

        if (!clipped)
            clipped = RowsRange(begin, end);
        else
            clipped->end = end; /// extend the coarse single-interval cover to the last overlap
    }

    return clipped;
}

PostingList TextIndexAnalyzer::ReadableRows::clipPostings(const PostingList & postings)
{
    if (ranges_bitmap.isEmpty())
    {
        /// Lazily build the single combined bitmap of readable rows used to clip token postings.
        /// `addRangeClosed` stores contiguous ranges as run containers, so this stays compact (O(number of ranges)).
        for (const auto & range : ranges)
            ranges_bitmap.addRangeClosed(static_cast<UInt32>(range.begin), static_cast<UInt32>(range.end));
    }

    return postings & ranges_bitmap;
}

size_t TextIndexAnalyzer::ReadableRows::getSizeInBytes() const
{
    return ranges.capacity() * sizeof(RowsRange) + ranges_bitmap.getSizeInBytes();
}

void TextIndexAnalyzer::QueryBuilder::markFailed()
{
    is_failed = true;
    postings.reset();
    rows_range.reset();
    num_live_tokens = 0;
}

void TextIndexAnalyzer::QueryBuilder::markBypassed()
{
    is_bypassed = true;
    /// Keep `postings` and `rows_range` for index analysis in `mayBeTrueOnGranule`.
    /// Bypassing a query makes sense only for direct read optimization.
}

void TextIndexAnalyzer::QueryBuilder::addMissingToken(std::string_view token)
{
    tokens.erase(token);

    if (query->getSearchMode() == TextSearchMode::All || query->getSearchMode() == TextSearchMode::Phrase)
    {
        markFailed();
        return;
    }

    /// `Any` mode fails once none of its declared tokens can contribute.
    /// Pattern queries discover tokens dynamically, so the count applies only to pure-token queries.
    if (query->getPatterns().empty())
    {
        if (num_live_tokens > 0)
            --num_live_tokens;

        if (num_live_tokens == 0)
            markFailed();
    }
}

void TextIndexAnalyzer::QueryBuilder::addTokenInfo(std::string_view token, TokenPostingsInfoPtr token_info, RowsRange token_rows_range)
{
    if (is_failed || tokens.contains(token))
        return;

    tokens[token] = token_info;
    addRowsRange(token_rows_range);
}

void TextIndexAnalyzer::QueryBuilder::addRowsRange(RowsRange token_rows_range)
{
    if (is_failed)
        return;

    if (!rows_range)
    {
        rows_range = token_rows_range;
    }
    else if (query->getSearchMode() == TextSearchMode::Any)
    {
        rows_range = rows_range->unionWith(token_rows_range);
    }
    else if (query->getSearchMode() == TextSearchMode::All || query->getSearchMode() == TextSearchMode::Phrase)
    {
        rows_range = rows_range->intersectWith(token_rows_range);

        if (!rows_range)
            markFailed();
    }
}

void TextIndexAnalyzer::QueryBuilder::addPostings(const PostingList & token_postings)
{
    if (is_failed)
        return;

    ++num_read_postings;

    if (!postings)
        postings = token_postings;
    else if (query->getSearchMode() == TextSearchMode::Any)
        *postings |= token_postings;
    else
        *postings &= token_postings;

    /// `All` mode fails as soon as the running intersection of readable postings becomes empty.
    bool need_all_tokens = query->getSearchMode() == TextSearchMode::All || query->getSearchMode() == TextSearchMode::Phrase;
    if (need_all_tokens && postings->cardinality() == 0)
        markFailed();
}

TextIndexAnalyzer::TextIndexAnalyzer(const MergeTreeIndexConditionText & condition_text)
{
    global_search_mode = condition_text.getGlobalSearchMode();

    for (const auto & [hash, query] : condition_text.getAllSearchQueries())
    {
        auto & query_builder = query_builders[hash];
        query_builder.query = query;

        for (const auto & token : query->getTokens())
        {
            if (queries_by_token[token].insert(hash).second)
                ++query_builder.num_live_tokens;
        }

        for (const auto & pattern : query->getPatterns())
            queries_by_pattern[&pattern].insert(hash);
    }
}

const TextIndexAnalyzer::QueryBuilder & TextIndexAnalyzer::getQueryBuilder(const TextSearchQuery & query) const
{
    auto hash = query.getHash();
    auto it = query_builders.find(hash);

    if (it == query_builders.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query builder not found for text search query with function '{}'", query.getFunctionName());

    return it->second;
}

void TextIndexAnalyzer::addMissingToken(std::string_view token)
{
    missing_tokens.emplace(token);

    processTokenOperation(token, [&](QueryBuilder & query_builder)
    {
        query_builder.addMissingToken(token);
    });
}

void TextIndexAnalyzer::addTokenInfo(std::string_view token, TokenPostingsInfoPtr token_info)
{
    all_token_infos[token] = token_info;

    /// Clip the token's row range to the readable rows once.
    chassert(!token_info->ranges.empty());
    RowsRange token_rows_range(token_info->ranges.front().begin, token_info->ranges.back().end);

    if (readable_rows)
    {
        auto clipped_range = readable_rows->clipRowsRange(token_rows_range);

        if (!clipped_range)
        {
            processTokenOperation(token, [&](QueryBuilder & query_builder)
            {
                query_builder.addMissingToken(token);
            });

            queries_by_token.erase(token);
            return;
        }

        token_rows_range = *clipped_range;
    }

    processTokenOperation(token, [&](QueryBuilder & query_builder)
    {
        query_builder.addTokenInfo(token, token_info, token_rows_range);
    });

    if (token_info->embedded_postings)
        addPostings(token, token_info->embedded_postings);
}

void TextIndexAnalyzer::addPostings(std::string_view token, PostingListPtr postings)
{
    tokens_with_postings.emplace(token);

    /// Clip the postings to the readable rows once.
    std::optional<PostingList> clipped_postings;
    const auto * postings_ptr = postings.get();

    if (readable_rows)
    {
        clipped_postings = readable_rows->clipPostings(*postings);

        if (clipped_postings->cardinality() == 0)
        {
            processTokenOperation(token, [&](QueryBuilder & query_builder)
            {
                query_builder.addMissingToken(token);
            });

            queries_by_token.erase(token);
            return;
        }

        postings_ptr = &*clipped_postings;
    }

    processTokenOperation(token, [&](QueryBuilder & query_builder)
    {
        query_builder.addPostings(*postings_ptr);
    });
}

void TextIndexAnalyzer::setReadableRows(std::vector<RowsRange> readable_ranges)
{
    readable_rows.reset();

    if (!readable_ranges.empty())
        readable_rows.emplace(std::move(readable_ranges));
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

bool TextIndexAnalyzer::hasReadPostings(std::string_view token) const
{
    return tokens_with_postings.contains(token);
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
        auto & query_builder = query_builders.at(query_hash);
        query_builder.markBypassed();

        for (const auto & [query_token, _] : query_builder.tokens)
            queries_by_token[query_token].erase(query_hash);
    }
}

double TextIndexAnalyzer::estimateQueryCardinality(const QueryBuilder & query_builder, size_t total_rows) const
{
    const auto & query = *query_builder.query;
    chassert(!query.getTokens().empty());
    const double n = static_cast<double>(total_rows);

    switch (query.getSearchMode())
    {
        case TextSearchMode::All:
        /// A phrase requires all its tokens to be present.
        case TextSearchMode::Phrase:
        {
            /// |intersection| ≈ |C_read| * prod(|Ai|/n) over tokens whose postings are still unread.
            /// When no postings have been read yet, treat the read intersection as the universe (n).
            /// In log-space: log = log(|C_read|) + sum(log(|Ai|)) - num_unread * log(n).
            double log_cardinality = query_builder.postings
                ? std::log(static_cast<double>(query_builder.postings->cardinality()))
                : std::log(n);

            size_t num_unread = 0;
            for (const auto & token : query.getTokens())
            {
                auto it = query_builder.tokens.find(token);
                if (it == query_builder.tokens.end())
                    return 0;

                if (hasReadPostings(token))
                    continue;

                log_cardinality += std::log(static_cast<double>(it->second->cardinality));
                ++num_unread;
            }

            log_cardinality -= static_cast<double>(num_unread) * std::log(n);
            return std::exp(log_cardinality);
        }
        case TextSearchMode::Any:
        {
            /// |union| ≈ n * (1 - (1 - |C_read|/n) * prod(1 - |Ai|/n)) over tokens whose postings are still unread.
            double not_in_any = query_builder.postings
                ? 1.0 - static_cast<double>(query_builder.postings->cardinality()) / n
                : 1.0;

            for (const auto & token : query.getTokens())
            {
                auto it = query_builder.tokens.find(token);
                if (it != query_builder.tokens.end() && hasReadPostings(token))
                    continue;

                /// Same reasoning as the prior reader-side estimate: a token absent from the
                /// sparse index was filtered as too common at build time ⟹ treat it as covering
                /// all rows, which makes the union saturate at n.
                double token_cardinality = (it == query_builder.tokens.end())
                    ? n
                    : static_cast<double>(it->second->cardinality);

                not_in_any *= (1.0 - token_cardinality / n);
            }

            return n * (1.0 - not_in_any);
        }
    }
}

void TextIndexAnalyzer::analyzeCardinalitiesAndBypassHints(double selectivity_threshold, size_t total_rows)
{
    if (total_rows == 0)
        return;

    const double cardinality_threshold = static_cast<double>(total_rows) * selectivity_threshold;

    for (auto & [_, query_builder] : query_builders)
    {
        if (query_builder.is_failed || query_builder.is_bypassed)
            continue;

        const auto & query = *query_builder.query;
        if (query.getDirectReadMode() != TextIndexDirectReadMode::Hint)
            continue;

        /// Pure-pattern queries have no declared tokens at parse time; their tokens are
        /// discovered dynamically during dictionary scan. Skip the cardinality check in
        /// that case — it would have no inputs to work with.
        if (query.getTokens().empty())
            continue;

        double estimated_cardinality = estimateQueryCardinality(query_builder, total_rows);

        if (estimated_cardinality <= cardinality_threshold)
        {
            ProfileEvents::increment(ProfileEvents::TextIndexUseHint);
        }
        else
        {
            /// Drop the query from `queries_by_token` so pattern discovery and `isTokenNeeded`
            /// stop reactivating it; `postings`/`rows_range` are preserved for `mayBeTrueOnGranule`.
            query_builder.markBypassed();
            ProfileEvents::increment(ProfileEvents::TextIndexDiscardHint);

            auto hash = query.getHash();
            for (const auto & query_token : query.getTokens())
                queries_by_token[query_token].erase(hash);

            for (const auto & [query_token, _] : query_builder.tokens)
                queries_by_token[query_token].erase(hash);
        }
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
        auto & query_builder = query_builders.at(query_hash);
        if (query_builder.is_failed || query_builder.is_bypassed)
            continue;

        operation(query_builder);

        if (query_builder.is_failed)
        {
            if (global_search_mode == TextSearchMode::All)
                always_false = true;

            /// Erase the failed query for the full declared token set so yet-unseen tokens stop passing isTokenNeeded.
            for (const auto & query_token : query_builder.query->getTokens())
                queries_by_token[query_token].erase(query_hash);

            /// Also erase for already-discovered dynamic pattern tokens (not in `query->getTokens`).
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

    /// all_token_infos: map<String, TokenPostingsInfoPtr>.
    result += estimateAbslFlatContainerBytes(all_token_infos);
    for (const auto & [key, _] : all_token_infos)
        result += key.capacity();

    /// missing_tokens: set<String>.
    result += estimateAbslFlatContainerBytes(missing_tokens);
    for (const auto & token : missing_tokens)
        result += token.capacity();

    /// tokens_with_postings: set<String>.
    result += estimateAbslFlatContainerBytes(tokens_with_postings);
    for (const auto & token : tokens_with_postings)
        result += token.capacity();

    result += readable_rows.has_value() ? readable_rows->getSizeInBytes() : 0;
    return result;
}

}
