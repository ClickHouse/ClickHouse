#pragma once

#include <Storages/Statistics/StatisticsFwd.h>
#include <Storages/Statistics/StatisticsUniqBuildProbe.h>
#include <Storages/StatisticsDescription.h>

#include <map>
#include <optional>
#include <vector>

namespace DB
{

struct StatisticsBuildOptions;

using StatisticsMap = std::map<StatisticsType, StatisticsPtr>;

struct AssumedAllDistinctBuildDecision
{
    std::vector<StatisticsType> types_to_replace;
    UInt64 initial_cardinality = 0;
    std::optional<ColumnPtr> build_column_override;
    String reason;

    bool replaces(StatisticsType type) const;
    void applyReplacements(ColumnStatisticsDescription & stats_desc, StatisticsMap & stats) const;
};

struct AssumedAllDistinctMergeDecision
{
    UInt64 cardinality = 0;

    bool shouldSkipRegularMerge(StatisticsType type) const;
    void apply(ColumnStatisticsDescription & stats_desc, StatisticsMap & stats) const;
};

/// Owns the policy for switching logical uniq-like statistics to the assumed-all-distinct
/// materialization. The policy intentionally applies to both implicit and explicit logical uniq
/// statistics when the build settings request it; explicit `uniq(assumed_all_distinct)` statistics
/// are already represented by StatisticsAssumedAllDistinct and are simply built normally.
class UniqAssumedAllDistinctPolicy
{
public:
    std::optional<AssumedAllDistinctBuildDecision> decideBuild(
        const ColumnStatisticsDescription & stats_desc,
        const StatisticsMap & stats,
        const ColumnPtr & column,
        const StatisticsBuildOptions & options);

    static std::optional<AssumedAllDistinctMergeDecision> decideMerge(
        const StatisticsMap & stats, const StatisticsMap & other_stats);
    static bool isStructureCompatible(StatisticsType type, const IStatistics & lhs, const IStatistics & rhs);

private:
    StatisticsUniqStringProbe string_probe;
};

}
