#include <Storages/MergeTree/WhatIfStatisticalEstimator.h>

#include <Storages/MergeTree/WhatIfFilterAnalysis.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/StatisticsCache.h>
#include <Storages/ProjectionsDescription.h>
#include <Interpreters/Context.h>

#include <algorithm>
#include <cmath>
#include <optional>

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 index_granularity;
    extern const MergeTreeSettingsUInt64 index_granularity_bytes;
}

namespace
{

/// the share of the rows of `parts` that pass `filter`, from the statistics of `columns`
/// with `every_part`, a part without the statistics of every column gives no estimate, else one part with statistics is enough
std::optional<double> selectivityFromStatistics(
    const RangesInDataParts & parts,
    const NameSet & columns,
    const ActionsDAG::Node * filter,
    const StorageMetadataPtr & metadata,
    bool every_part,
    ContextPtr context)
{
    ConditionSelectivityEstimatorBuilder builder(context);
    bool has_any_stats = false;

    auto statistics_cache = context->getStatisticsCache();
    for (const auto & part : parts)
    {
        /// Every analyzed part counts towards the baseline row count, like in `getConditionSelectivityEstimator`,
        /// even if it has no statistics for the filter columns: `analysis.selected_marks` covers all parts.
        builder.incrementRowCount(part.data_part->rows_count);
        auto stats = part.data_part->loadStatistics(columns, statistics_cache.get());
        if (every_part && std::ranges::any_of(columns, [&](const String & column) { return !stats.contains(column); }))
            return {};
        for (const auto & [column_name, stat] : stats)
            builder.addStatistics(column_name, stat);
        has_any_stats |= !stats.empty();
    }

    if (!has_any_stats)
        return {};

    auto estimator = builder.getEstimator();
    if (!estimator)
        return {};

    auto profile = estimator->estimateRelationProfile(metadata, filter);
    auto unfiltered = estimator->estimateRelationProfile();
    if (unfiltered.rows == 0)
        return {};

    return std::min(1.0, static_cast<double>(profile.rows) / static_cast<double>(unfiltered.rows));
}

}

bool tryEstimateWithStatistics(
    WhatIfCandidateResult & result,
    const MergeTreeIndexPtr & index_helper,
    ReadFromMergeTree * read_step,
    const ReadFromMergeTree::AnalysisResult & analysis,
    const RangesInDataParts & parts,
    const ActionsDAG::Node * filter_node,
    ContextPtr context)
{
    auto metadata = read_step->getStorageMetadata();

    if (!metadata->hasStatistics())
        return false;

    if (parts.empty())
        return false;

    /// Only when filter touches just the index's columns, else other columns'
    /// selectivity leaks into the skip ratio
    NameSet index_columns_set;
    for (const auto & col : index_helper->getColumnsRequiredForIndexCalc())
        index_columns_set.insert(col);

    NameSet filter_input_columns;
    collectFilterInputColumns(filter_node, filter_input_columns);

    for (const auto & col : filter_input_columns)
        if (!index_columns_set.contains(col))
            return false;

    const auto selectivity = selectivityFromStatistics(parts, filter_input_columns, filter_node, metadata, /* every_part */ false, context);
    if (!selectivity)
        return false;

    /// Row-level selectivity as upper bound for granule-level skip ratio
    result.skip_ratio = 1.0 - *selectivity;
    result.estimated_marks = std::max<UInt64>(1, static_cast<UInt64>(static_cast<double>(analysis.selected_marks) * *selectivity));
    result.estimate_source = WhatIfCandidateResult::Statistical;
    return true;
}

bool tryEstimateProjectionWithStatistics(
    WhatIfCandidateResult & result,
    const ProjectionDescription & projection,
    ReadFromMergeTree * read_step,
    const RangesInDataParts & parts,
    ContextPtr context)
{
    auto metadata = read_step->getStorageMetadata();
    const auto & filter_dag = read_step->getFilterActionsDAG();
    const auto & key_columns = projection.metadata->getSortingKey().column_names;
    if (!metadata->hasStatistics() || !filter_dag || parts.empty() || key_columns.empty())
        return false;

    /// the projection keeps the matching rows next to each other only for a filter on the first column of its key
    const ActionsDAGWithInversionPushDown predicate_dag(filter_dag->getOutputs().front(), context, /* boolean_context */ true);
    NameSet filter_columns;
    collectFilterInputColumns(predicate_dag.predicate, filter_columns);
    if (filter_columns != NameSet{key_columns.front()})
        return false;

    /// a part with no statistics would make the estimate cover only a part of the read
    const auto selectivity = selectivityFromStatistics(parts, filter_columns, predicate_dag.predicate, metadata, /* every_part */ true, context);
    if (!selectivity)
        return false;

    const auto & data = read_step->getMergeTreeData();
    const auto mt_settings = data.getSettings(&projection.settings_changes);
    const UInt64 fixed_rows = (*mt_settings)[MergeTreeSetting::index_granularity];
    const UInt64 granule_bytes = data.canUseAdaptiveGranularity() ? (*mt_settings)[MergeTreeSetting::index_granularity_bytes] : 0;

    UInt64 marks = 0;
    UInt64 rows = 0;
    for (const auto & part : parts)
    {
        const UInt64 part_rows = part.data_part->rows_count;
        if (part_rows == 0)
            continue;

        /// the writer ends a granule at `index_granularity_bytes` of the columns the projection stores
        UInt64 granule_rows = fixed_rows;
        if (granule_bytes != 0)
        {
            UInt64 bytes = 0;
            for (const auto & column : projection.metadata->getColumns().getAllPhysical())
                bytes += part.data_part->getColumnSize(column.name).data_uncompressed;
            if (bytes != 0)
                granule_rows = std::clamp<UInt64>(granule_bytes * part_rows / bytes, 1, fixed_rows);
        }

        /// the first matching row is somewhere in a granule, so the matching rows touch one more granule on average
        const UInt64 part_marks = (part_rows + granule_rows - 1) / granule_rows;
        const double matching_rows = *selectivity * static_cast<double>(part_rows);
        const UInt64 read = std::min<UInt64>(
            part_marks, 1 + static_cast<UInt64>(std::round(std::max(matching_rows - 1.0, 0.0) / static_cast<double>(granule_rows))));
        marks += read;
        rows += std::min(part_rows, read * granule_rows);
    }

    result.estimated_marks = marks;
    result.estimated_rows = rows;
    result.estimate_source = WhatIfCandidateResult::Statistical;
    return true;
}

}
