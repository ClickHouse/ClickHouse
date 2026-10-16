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

/// Returns the preferred cardinality estimator: any real sketch over any assumed-all-distinct
/// statistic (the latter is only a row-count heuristic), then Uniq over UniqV2 (Uniq has better
/// precision). Shared by ColumnStatistics estimation and the merge policy.
const IStatistics * findPreferredCardinalityStatistics(const StatisticsMap & stats);

struct AssumedAllDistinctBuildDecision
{
    std::vector<StatisticsType> types_to_replace;
    UInt64 initial_cardinality = 0;
    /// Rows of the current block the replacement statistics still need to count.
    /// nullptr means the probe already accounted for every row of the block.
    ColumnPtr rows_to_count;
    String reason;

    bool replaces(StatisticsType type) const;
    void applyReplacements(ColumnStatisticsDescription & stats_desc, StatisticsMap & stats) const;
};

struct AssumedAllDistinctMergeDecision
{
    /// Uniq-like types where at least one merge side is assumed-all-distinct, mapped to the summed
    /// cardinality of both sides. Types where both sides carry real sketches merge normally.
    std::map<StatisticsType, UInt64> types_to_replace;

    bool shouldSkipRegularMerge(StatisticsType type) const;
    void apply(ColumnStatisticsDescription & stats_desc, StatisticsMap & stats) const;
};

/// Policy for switching logical uniq-like statistics to the assumed-all-distinct materialization.
/// The build settings only convert statistics added *implicitly* through `auto_statistics_types`;
/// an explicit `STATISTICS(uniq_v2)` clause is a request for a real sketch and is left alone,
/// while an explicit `STATISTICS(uniq_v2(assumed_all_distinct))` is created as assumed from the
/// start and never reaches this policy.
class UniqAssumedAllDistinctPolicy
{
public:
    std::optional<AssumedAllDistinctBuildDecision> decideBuild(
        const ColumnStatisticsDescription & stats_desc,
        const StatisticsMap & stats,
        const ColumnPtr & column,
        const StatisticsBuildOptions & options);

    static std::optional<AssumedAllDistinctMergeDecision> decideMerge(const StatisticsMap & stats, const StatisticsMap & other_stats);
    static bool isStructureCompatible(StatisticsType type, const IStatistics & lhs, const IStatistics & rhs);

    /// True when `part_stats` carries an assumed-all-distinct uniq statistic that is no longer
    /// requested: the table metadata (`desired_desc`) does not declare it explicitly, and the
    /// current build `options` disable the assumption for the column type altogether (e.g. the user
    /// turned `auto_statistics_assume_floats_distinct` off). Merges use this to rebuild such
    /// statistics from data instead of propagating the assumption forever.
    /// This is a metadata-only check: the data-dependent guards (the probed average string length
    /// against `auto_statistics_long_string_distinct_min_length`, the ratio of default rows against
    /// `ratio_of_defaults_for_sparse_serialization`) are not re-evaluated, because that would need
    /// the part's data and thus a rebuild of every statistic of the column on every merge. After
    /// changing those thresholds, existing assumptions are kept until `MATERIALIZE STATISTICS`.
    static bool hasStaleAssumedStatistics(
        const ColumnStatisticsDescription & desired_desc, const StatisticsMap & part_stats, const StatisticsBuildOptions & options);

private:
    /// Set once the build of this part has decided to keep the real sketches (the first eligible
    /// Float block reached the default-ratio threshold, or the string probe rejected the assumption).
    /// The decision is final for the part: converting on a later block would throw away everything
    /// the real sketch has accumulated so far and restart the assumed cardinality from that block.
    bool keep_real_sketches = false;
    StatisticsUniqStringProbe string_probe;
};

}
