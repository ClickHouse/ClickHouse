#include <Storages/MergeTree/WhatIfStatisticalEstimator.h>

#include <Storages/MergeTree/WhatIfFilterAnalysis.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Common/Exception.h>

namespace DB
{

bool tryEstimateWithStatistics(
    WhatIfIndexEstimator::IndexResult & result,
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

    Names required_stats_columns(filter_input_columns.begin(), filter_input_columns.end());

    ConditionSelectivityEstimatorBuilder builder(context);
    bool all_parts_loaded = true;

    for (const auto & part : parts)
    {
        if (!part.data_part)
            return false;

        try
        {
            auto stats = part.data_part->loadStatistics(required_stats_columns);
            builder.addDataPartStatistics(part.data_part, stats);
        }
        catch (const Exception &) /// Ok — statistical estimation is best-effort
        {
            all_parts_loaded = false;
            tryLogCurrentException(__PRETTY_FUNCTION__);
            break;
        }
    }

    if (!all_parts_loaded)
        return false;

    auto estimator = builder.getEstimator();
    if (!estimator)
        return false;

    if (!estimator->hasStatisticsFor(metadata, required_stats_columns))
        return false;

    auto profile = estimator->estimateRelationProfile(metadata, filter_node);
    auto unfiltered = estimator->estimateRelationProfile();
    if (unfiltered.rows == 0)
        return false;

    /// Row-level selectivity as upper bound for granule-level skip ratio
    double selectivity = std::min(1.0, static_cast<double>(profile.rows) / static_cast<double>(unfiltered.rows));
    result.skip_ratio = 1.0 - selectivity;
    result.estimated_marks = std::max<UInt64>(1, static_cast<UInt64>(static_cast<double>(analysis.selected_marks) * selectivity));
    result.estimate_source = "statistical";
    return true;
}

}
