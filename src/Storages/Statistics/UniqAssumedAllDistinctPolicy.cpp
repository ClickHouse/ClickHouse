#include "UniqAssumedAllDistinctPolicy.h"
#include "Statistics.h"
#include "StatisticsAssumedAllDistinct.h"

#include <Columns/IColumn.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/logger_useful.h>

#include <algorithm>

namespace DB
{

const IStatistics * findPreferredCardinalityStatistics(const StatisticsMap & stats)
{
    if (auto it = stats.find(StatisticsType::Uniq); it != stats.end())
        return it->second.get();
    if (auto it = stats.find(StatisticsType::UniqV2); it != stats.end())
        return it->second.get();
    return nullptr;
}

namespace
{

/// Cardinality contribution of one merge side for a uniq-like `type`. A side lacking the type
/// contributes its best available cardinality estimate so that e.g. an old part carrying only
/// `uniq` still counts towards the merged `uniq_v2` assumption.
UInt64 estimateCardinalityForType(const StatisticsMap & stats, StatisticsType type)
{
    if (auto it = stats.find(type); it != stats.end())
        return it->second->estimateCardinality();
    if (const auto * cardinality_stats = findPreferredCardinalityStatistics(stats))
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

/// Uniq-like statistics the build settings may convert. Only *implicit* statistics (added through
/// `auto_statistics_types`) qualify: an explicit `STATISTICS(uniq...)` clause asks for a real
/// sketch, and users opt into the assumption per column with `STATISTICS(uniq(assumed_all_distinct))`.
std::vector<StatisticsType> getConvertibleUniqStatistics(const StatisticsMap & stats)
{
    std::vector<StatisticsType> result;
    for (const auto & [type, stat_ptr] : stats)
    {
        if (isUniqLikeStatisticsType(type) && !isAssumedAllDistinctStatistics(*stat_ptr) && stat_ptr->getDescription().is_implicit)
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

/// Whether the build settings could (re-)produce the assumed-all-distinct materialization for a
/// column of `data_type`. Used to detect assumptions that became stale after a settings change.
bool buildOptionsMayAssumeAllDistinct(const DataTypePtr & data_type, const StatisticsBuildOptions & options)
{
    if (!data_type)
        return true; /// Without a type we cannot rule the assumption out; stay conservative.

    const auto inner_data_type = removeLowCardinalityAndNullable(removeNullable(data_type));
    if (WhichDataType(inner_data_type).isFloat())
        return options.assume_floats_distinct;
    if (isStringOrFixedString(inner_data_type))
        return options.assume_long_strings_distinct && options.long_string_distinct_probe_rows != 0;
    return false;
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
    return types_to_replace.contains(type);
}

void AssumedAllDistinctMergeDecision::apply(ColumnStatisticsDescription & stats_desc, StatisticsMap & stats) const
{
    for (const auto & [type, cardinality] : types_to_replace)
        setAssumedAllDistinctStatistics(stats_desc, stats, type, cardinality);
}

std::optional<AssumedAllDistinctBuildDecision> UniqAssumedAllDistinctPolicy::decideBuild(
    const ColumnStatisticsDescription & stats_desc,
    const StatisticsMap & stats,
    const ColumnPtr & column,
    const StatisticsBuildOptions & options)
{
    auto types_to_replace = getConvertibleUniqStatistics(stats);
    /// Note: blocks rejected here (e.g. a sparse block in a mixed stream) are built into the real
    /// sketch but never probed; if a later block flips the probe to the assumption, those rows are
    /// not part of the assumed cardinality. That undercount is acceptable for an estimate.
    if (types_to_replace.empty() || !stats_desc.data_type || !canUseAssumedAllDistinctForUniqBuild(column))
        return std::nullopt;

    const auto inner_data_type = removeLowCardinalityAndNullable(removeNullable(stats_desc.data_type));
    const bool may_have_nulls = isNullableOrLowCardinalityNullable(stats_desc.data_type);

    if (options.assume_floats_distinct && WhichDataType(inner_data_type).isFloat())
    {
        /// Data-based counterpart of the ColumnSparse representation check: on the insert path a
        /// default-dominated column arrives as a full (not yet sparse) column, yet repeated
        /// defaults obviously violate the all-distinct assumption. NULL rows count towards the
        /// default ratio of a Nullable column, which errs on the side of a real sketch.
        /// The decision is made once per part (on the first eligible block), like the string probe.
        const Float64 default_ratio = column->getRatioOfDefaultRows();
        if (default_ratio >= options.max_default_ratio_for_assumed_distinct)
        {
            LOG_TRACE(
                getLogger("ColumnStatistics"),
                "Keeping cardinality statistics ({}) for column type {}: ratio of default rows {} reaches the sparse threshold {}",
                formatStatisticsTypes(types_to_replace),
                stats_desc.data_type->getName(),
                default_ratio,
                options.max_default_ratio_for_assumed_distinct);
            return std::nullopt;
        }

        AssumedAllDistinctBuildDecision decision;
        decision.types_to_replace = std::move(types_to_replace);
        decision.rows_to_count = column;
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
        decision.types_to_replace = std::move(types_to_replace);
        decision.initial_cardinality = probe_result.assumed_cardinality;
        decision.rows_to_count = probe_result.unprobed_column_tail;
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

std::optional<AssumedAllDistinctMergeDecision>
UniqAssumedAllDistinctPolicy::decideMerge(const StatisticsMap & stats, const StatisticsMap & other_stats)
{
    /// A uniq-like type is replaced by an assumed-all-distinct statistic iff at least one side
    /// already carries the assumption for that type; a pair of real sketches merges normally,
    /// so an explicit sketch is never degraded by an assumed sibling type.
    AssumedAllDistinctMergeDecision decision;
    for (const auto * side : {&stats, &other_stats})
        for (const auto & [type, stat_ptr] : *side)
            if (isUniqLikeStatisticsType(type) && isAssumedAllDistinctStatistics(*stat_ptr) && !decision.types_to_replace.contains(type))
                decision.types_to_replace.emplace(
                    type, estimateCardinalityForType(stats, type) + estimateCardinalityForType(other_stats, type));

    if (decision.types_to_replace.empty())
        return std::nullopt;
    return decision;
}

bool UniqAssumedAllDistinctPolicy::isStructureCompatible(StatisticsType type, const IStatistics & lhs, const IStatistics & rhs)
{
    return isUniqLikeStatisticsType(type) && (isAssumedAllDistinctStatistics(lhs) || isAssumedAllDistinctStatistics(rhs));
}

bool UniqAssumedAllDistinctPolicy::hasStaleAssumedStatistics(
    const ColumnStatisticsDescription & desired_desc, const StatisticsMap & part_stats, const StatisticsBuildOptions & options)
{
    for (const auto & [type, stat_ptr] : part_stats)
    {
        if (!isUniqLikeStatisticsType(type) || !isAssumedAllDistinctStatistics(*stat_ptr))
            continue;

        if (auto it = desired_desc.types_to_desc.find(type); it != desired_desc.types_to_desc.end())
        {
            /// The table metadata still asks for this materialization explicitly.
            if (it->second.materialization == StatisticsMaterialization::AssumedAllDistinct)
                continue;

            /// The build settings could re-derive the assumption for this implicit statistic
            /// anyway, so merging the existing assumption is cheaper than a rebuild. (For string
            /// columns the rebuilt probe could decide differently, e.g. after a threshold change;
            /// we accept the existing assumption in that case rather than rebuilding every merge.)
            if (it->second.is_implicit && buildOptionsMayAssumeAllDistinct(desired_desc.data_type, options))
                continue;
        }

        return true;
    }
    return false;
}

}
