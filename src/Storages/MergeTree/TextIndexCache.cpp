#include <Storages/MergeTree/TextIndexCache.h>

namespace DB
{

TokensCardinalitiesCache::TokensCardinalitiesCache(std::vector<String> all_search_tokens_)
    : all_search_tokens(std::move(all_search_tokens_))
{
    for (const auto & token : all_search_tokens)
        cardinalities.emplace(token, CardinalityAggregate{});
}

void TokensCardinalitiesCache::update(const TokenToPostingsInfosMap & token_infos, const NameSet & missing_tokens, size_t total_rows)
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
    CardinalitiesMap current_cardinalities;

    {
        std::lock_guard lock(mutex);
        current_cardinalities = cardinalities;
    }

    std::ranges::sort(tokens, [&current_cardinalities](const auto & lhs_token, const auto & rhs_token)
    {
        const auto & lhs_cardinality = current_cardinalities.at(lhs_token);
        const auto & rhs_cardinality = current_cardinalities.at(rhs_token);

        double lhs_ratio = lhs_cardinality.checked_rows > 0 ? static_cast<double>(lhs_cardinality.cardinality) / static_cast<double>(lhs_cardinality.checked_rows) : 1.0;
        double rhs_ratio = rhs_cardinality.checked_rows > 0 ? static_cast<double>(rhs_cardinality.cardinality) / static_cast<double>(rhs_cardinality.checked_rows) : 1.0;

        return std::tie(lhs_ratio, lhs_token) < std::tie(rhs_ratio, rhs_token);
    });
}

}
