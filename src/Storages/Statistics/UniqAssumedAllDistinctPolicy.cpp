#include "UniqAssumedAllDistinctPolicy.h"
#include "Statistics.h"
#include "StatisticsAssumedAllDistinct.h"

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/logger_useful.h>

#include <algorithm>

namespace DB
{

namespace
{

/// Returns the first available logical uniq-like cardinality estimator.
/// Keeps the historical Uniq-over-UniqV2 preference when both exist.
const IStatistics * findCardinalityStats(const StatisticsMap & m)
{
    if (auto it = m.find(StatisticsType::Uniq); it != m.end())
        return it->second.get();
    if (auto it = m.find(StatisticsType::UniqV2); it != m.end())
        return it->second.get();
    return nullptr;
}

bool hasAssumedAllDistinctUniqLikeStatistics(const StatisticsMap & m)
{
    return std::ranges::any_of(
        m, [](const auto & entry) { return isUniqLikeStatisticsType(entry.first) && isAssumedAllDistinctStatistics(*entry.second); });
}

UInt64 estimateCardinalityForAssumedAllDistinctMerge(const StatisticsMap & m)
{
    for (auto preferred_type : {StatisticsType::Uniq, StatisticsType::UniqV2})
    {
        if (auto it = m.find(preferred_type); it != m.end() && isAssumedAllDistinctStatistics(*it->second))
            return it->second->estimateCardinality();
    }

    if (const auto * cardinality_stats = findCardinalityStats(m))
        return cardinality_stats->estimateCardinality();
    return 0;
}

String formatStatisticsTypes(const std::vector<StatisticsType> & types)
{
    String result;
    for (auto type : types)
    {
        if (!result.empty())
            result += ", ";
        result += statisticsTypeToString(type);
    }
    return result;
}

std::vector<StatisticsType> getUniqLikeStatisticsForAssumedAllDistinct(const StatisticsMap & stats)
{
    std::vector<StatisticsType> result;
    for (const auto & [type, stat_ptr] : stats)
    {
        /// Build settings materialize all logical uniq-like statistics as assumed-all-distinct,
        /// regardless of whether they came from auto_statistics_types or an explicit STATISTICS clause.
        if (isUniqLikeStatisticsType(type) && !isAssumedAllDistinctStatistics(*stat_ptr))
            result.push_back(type);
    }
    return result;
}

void setAssumedAllDistinctStatistics(
    ColumnStatisticsDescription & stats_desc, StatisticsMap & stats, StatisticsType type, UInt64 cardinality)
{
    chassert(isUniqLikeStatisticsType(type));

    SingleStatisticsDescription desc(
        type,
        makeStatisticsTypeAST(type, StatisticsMaterialization::AssumedAllDistinct),
        /*is_implicit_=*/false,
        StatisticsMaterialization::AssumedAllDistinct);

    if (auto desc_it = stats_desc.types_to_desc.find(type); desc_it != stats_desc.types_to_desc.end())
    {
        desc = desc_it->second;
        desc.materialization = StatisticsMaterialization::AssumedAllDistinct;
        desc.ast = makeStatisticsTypeAST(type, StatisticsMaterialization::AssumedAllDistinct);
    }

    stats_desc.types_to_desc.insert_or_assign(type, desc);
    stats[type] = createAssumedAllDistinctStatistics(desc, cardinality);
}

}

bool AssumedAllDistinctBuildDecision::replaces(StatisticsType type) const
{
    return std::ranges::find(types_to_replace, type) != types_to_replace.end();
}

void AssumedAllDistinctBuildDecision::applyReplacements(ColumnStatisticsDescription & stats_desc, StatisticsMap & stats) const
{
    for (auto type : types_to_replace)
        setAssumedAllDistinctStatistics(stats_desc, stats, type, initial_cardinality);

    LOG_TRACE(
        getLogger("ColumnStatistics"),
        "Replacing cardinality statistics ({}) with assumed_all_distinct for column type {}: {}; initial assumed cardinality {}",
        formatStatisticsTypes(types_to_replace),
        stats_desc.data_type ? stats_desc.data_type->getName() : "<unknown>",
        reason,
        initial_cardinality);
}

bool AssumedAllDistinctMergeDecision::shouldSkipRegularMerge(StatisticsType type) const
{
    return isUniqLikeStatisticsType(type);
}

void AssumedAllDistinctMergeDecision::apply(ColumnStatisticsDescription & stats_desc, StatisticsMap & stats) const
{
    std::vector<StatisticsType> uniq_like_types;
    for (const auto & [type, _] : stats)
        if (isUniqLikeStatisticsType(type))
            uniq_like_types.push_back(type);

    for (auto type : uniq_like_types)
        setAssumedAllDistinctStatistics(stats_desc, stats, type, cardinality);
}

std::optional<AssumedAllDistinctBuildDecision> UniqAssumedAllDistinctPolicy::decideBuild(
    const ColumnStatisticsDescription & stats_desc,
    const StatisticsMap & stats,
    const ColumnPtr & column,
    const StatisticsBuildOptions & options)
{
    auto types_to_replace = getUniqLikeStatisticsForAssumedAllDistinct(stats);
    if (types_to_replace.empty() || !stats_desc.data_type || !canUseAssumedAllDistinctForUniqBuild(column))
        return std::nullopt;

    const auto inner_data_type = removeLowCardinalityAndNullable(removeNullable(stats_desc.data_type));
    const bool may_have_nulls = isNullableOrLowCardinalityNullable(stats_desc.data_type);

    if (options.assume_floats_distinct && WhichDataType(inner_data_type).isFloat())
    {
        AssumedAllDistinctBuildDecision decision;
        decision.types_to_replace = types_to_replace;
        decision.build_column_override = column;
        decision.reason = "auto_statistics_assume_floats_distinct is enabled for a Float column";
        return decision;
    }

    if (!string_probe.canProbe(column, inner_data_type, options))
        return std::nullopt;

    auto probe_result = string_probe.probe(column, may_have_nulls, options);
    if (probe_result.need_more_rows)
        return std::nullopt;

    const Float64 average_string_size = probe_result.total_probe_non_null_rows == 0
        ? 0.0
        : static_cast<Float64>(probe_result.total_probe_bytes) / static_cast<Float64>(probe_result.total_probe_non_null_rows);

    if (probe_result.assume_all_distinct)
    {
        AssumedAllDistinctBuildDecision decision;
        decision.types_to_replace = types_to_replace;
        decision.initial_cardinality = probe_result.assumed_cardinality;
        if (probe_result.unprobed_column_tail)
            decision.build_column_override = probe_result.unprobed_column_tail;
        decision.reason = "probed average string size " + std::to_string(average_string_size) + " reached threshold "
            + std::to_string(options.long_string_distinct_min_length);
        return decision;
    }

    LOG_TRACE(
        getLogger("ColumnStatistics"),
        "Keeping cardinality statistics ({}) for column type {} after probing {} rows: average string size {} is below threshold {}",
        formatStatisticsTypes(types_to_replace),
        stats_desc.data_type->getName(),
        probe_result.total_probe_rows,
        average_string_size,
        options.long_string_distinct_min_length);

    return std::nullopt;
}

std::optional<AssumedAllDistinctMergeDecision> UniqAssumedAllDistinctPolicy::decideMerge(
    const StatisticsMap & stats, const StatisticsMap & other_stats)
{
    if (!hasAssumedAllDistinctUniqLikeStatistics(stats) && !hasAssumedAllDistinctUniqLikeStatistics(other_stats))
        return std::nullopt;

    AssumedAllDistinctMergeDecision decision;
    decision.cardinality = estimateCardinalityForAssumedAllDistinctMerge(stats)
        + estimateCardinalityForAssumedAllDistinctMerge(other_stats);
    return decision;
}

bool UniqAssumedAllDistinctPolicy::isStructureCompatible(StatisticsType type, const IStatistics & lhs, const IStatistics & rhs)
{
    return isUniqLikeStatisticsType(type) && (isAssumedAllDistinctStatistics(lhs) || isAssumedAllDistinctStatistics(rhs));
}

}
