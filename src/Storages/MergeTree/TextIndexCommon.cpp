#include <Storages/MergeTree/TextIndexCommon.h>
#include <Storages/MergeTree/TextIndexCache.h>
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

std::vector<size_t> TokenPostingsInfo::getBlocksToRead(const RowsRange & range) const
{
    std::vector<size_t> blocks;
    for (size_t i = 0; i < ranges.size(); ++i)
    {
        if (ranges[i].intersects(range))
            blocks.emplace_back(i);
    }
    return blocks;
}

size_t TokenPostingsInfo::bytesAllocated() const
{
    return sizeof(TokenPostingsInfo)
        + offsets.capacity() * sizeof(UInt64)
        + ranges.size() * sizeof(RowsRange)
        + (embedded_postings ? embedded_postings->getSizeInBytes() : 0);
}

TokensCardinalitiesCache::TokensCardinalitiesCache(std::vector<String> all_search_tokens_, TextSearchMode global_search_mode_)
    : all_search_tokens(std::move(all_search_tokens_))
    , global_search_mode(global_search_mode_)
{
    for (const auto & token : all_search_tokens)
        cardinalities.emplace(token, CardinalityAggregate{});
}

void TokensCardinalitiesCache::update(const String & part_name, const TokenToPostingsInfosMap & token_infos, size_t num_tokens, size_t total_rows)
{
    std::lock_guard lock(mutex);
    num_tokens_by_part[part_name] = num_tokens;

    if (global_search_mode == TextSearchMode::Any)
        return;

    for (const auto & [token, token_info] : token_infos)
    {
        auto & cardinality_agg = cardinalities[String(token)];
        cardinality_agg.cardinality += token_info->cardinality;
        cardinality_agg.checked_rows += total_rows;
    }
}

void TokensCardinalitiesCache::sortTokens(std::vector<String> & tokens) const
{
    if (global_search_mode == TextSearchMode::Any)
    {
        std::ranges::sort(tokens);
        return;
    }

    CardinalitiesMap current_cardinalities;

    {
        std::lock_guard lock(mutex);
        current_cardinalities = cardinalities;
    }

    std::ranges::sort(tokens, [&current_cardinalities](const auto & lhs_token, const auto & rhs_token)
    {
        const auto & lhs_cardinality = current_cardinalities.at(lhs_token);
        const auto & rhs_cardinality = current_cardinalities.at(rhs_token);

        double lhs_ratio = lhs_cardinality.checked_rows > 0 ? static_cast<double>(lhs_cardinality.cardinality) / lhs_cardinality.checked_rows : 1.0;
        double rhs_ratio = rhs_cardinality.checked_rows > 0 ? static_cast<double>(rhs_cardinality.cardinality) / rhs_cardinality.checked_rows : 1.0;

        return std::tie(lhs_ratio, lhs_token) < std::tie(rhs_ratio, rhs_token);
    });
}

std::optional<size_t> TokensCardinalitiesCache::getNumTokens(const String & part_name) const
{
    std::lock_guard lock(mutex);
    auto it = num_tokens_by_part.find(part_name);

    if (it == num_tokens_by_part.end())
        return std::nullopt;

    return it->second;
}

}
