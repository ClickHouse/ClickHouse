#include <Storages/MergeTree/TextIndexCommon.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

bool RowsRange::intersects(const RowsRange & other) const
{
    return (begin <= other.begin && other.begin <= end) || (other.begin <= begin && begin <= other.end);
}

std::optional<RowsRange> RowsRange::intersectWith(const RowsRange & other) const
{
    if (!intersects(other))
        return std::nullopt;

    return RowsRange(std::max(begin, other.begin), std::min(end, other.end));
}

TokenInfosCache::TokenInfosCache(std::vector<String> all_search_tokens_, TextSearchMode global_search_mode_)
    : all_search_tokens(std::move(all_search_tokens_))
    , global_search_mode(global_search_mode_)
{
    for (const auto & token : all_search_tokens)
        cardinalities.emplace(token, CardinalityAggregate{});
}

bool TokenInfosCache::has(const String & part_name) const
{
    std::lock_guard lock(mutex);
    return cache.contains(part_name);
}

TokenToPostingsInfosPtr TokenInfosCache::get(const String & part_name) const
{
    std::lock_guard lock(mutex);
    auto it = cache.find(part_name);

    if (it == cache.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Token infos cache not found for part {}", part_name);

    return it->second;
}

void TokenInfosCache::set(const String & part_name, TokenToPostingsInfosPtr token_infos)
{
    std::lock_guard lock(mutex);
    cache[part_name] = std::move(token_infos);
}

void TokenInfosCache::updateCardinalities(const TokenToPostingsInfosMap & token_infos, size_t total_rows)
{
    std::lock_guard lock(mutex);

    for (const auto & [token, token_info] : token_infos)
    {
        auto & cardinality_agg = cardinalities[String(token)];
        cardinality_agg.cardinality += token_info.cardinality;
        cardinality_agg.checked_rows += total_rows;
    }
}

std::vector<String> TokenInfosCache::getOrderedTokens() const
{
    if (global_search_mode == TextSearchMode::Any)
    {
        chassert(std::ranges::is_sorted(all_search_tokens));
        return all_search_tokens;
    }

    std::vector<std::pair<String, CardinalityAggregate>> current_cardinalities;

    {
        std::lock_guard lock(mutex);
        current_cardinalities.reserve(cardinalities.size());

        for (const auto & [token, cardinality] : cardinalities)
            current_cardinalities.emplace_back(token, cardinality);
    }

    std::ranges::sort(current_cardinalities, [](const auto & lhs_pair, const auto & rhs_pair)
    {
        const auto & lhs = lhs_pair.second;
        const auto & rhs = rhs_pair.second;

        double lhs_ratio = lhs.checked_rows > 0 ? static_cast<double>(lhs.cardinality) / lhs.checked_rows : 1.0;
        double rhs_ratio = rhs.checked_rows > 0 ? static_cast<double>(rhs.cardinality) / rhs.checked_rows : 1.0;

        return std::tie(lhs_ratio, lhs_pair.first) < std::tie(rhs_ratio, rhs_pair.first);
    });

    std::vector<String> ordered_tokens;
    ordered_tokens.reserve(current_cardinalities.size());

    for (const auto & [token, cardinality] : current_cardinalities)
        ordered_tokens.push_back(token);

    return ordered_tokens;
}

}
