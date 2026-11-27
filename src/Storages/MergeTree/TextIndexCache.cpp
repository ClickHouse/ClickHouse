#include <Storages/MergeTree/TextIndexCache.h>

namespace DB
{

TokensCardinalitiesCache::TokensCardinalitiesCache(std::vector<String> all_search_tokens_)
    : all_search_tokens(std::move(all_search_tokens_))
{
    for (const auto & token : all_search_tokens)
        cardinalities.emplace(token, CardinalityAggregate{});
}

void TokensCardinalitiesCache::update(const TokenToPostingsInfosMap & token_infos, const absl::flat_hash_set<String> & missing_tokens, size_t total_rows)
{
    std::lock_guard lock(mutex);

    for (const auto & [token, token_info] : token_infos)
    {
        auto & cardinality_agg = cardinalities[token];
        cardinality_agg.cardinality += token_info->cardinality;
        cardinality_agg.checked_rows += total_rows;
    }

    for (const auto & token : missing_tokens)
    {
        auto & cardinality_agg = cardinalities[token];
        cardinality_agg.checked_rows += total_rows;
    }
}

void TokensCardinalitiesCache::sortTokens(std::vector<String> & tokens) const
{
    std::vector<std::pair<double, String>> token_ratios;
    token_ratios.reserve(tokens.size());

    {
        std::lock_guard lock(mutex);

        for (auto & token : tokens)
        {
            const auto & cardinality_agg = cardinalities.at(token);
            double ratio = cardinality_agg.checked_rows > 0 ? static_cast<double>(cardinality_agg.cardinality) / static_cast<double>(cardinality_agg.checked_rows) : 1.0;
            token_ratios.emplace_back(ratio, std::move(token));
        }
    }

    std::ranges::sort(token_ratios);

    for (size_t i = 0; i < tokens.size(); ++i)
        tokens[i] = std::move(token_ratios[i].second);
}

}
