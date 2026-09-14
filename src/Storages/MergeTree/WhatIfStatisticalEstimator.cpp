#include <Storages/MergeTree/WhatIfStatisticalEstimator.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Storages/MergeTree/WhatIfFilterAnalysis.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>

namespace DB
{

static String normalizeStatisticsColumnName(const String & column_name, const StorageMetadataPtr & metadata)
{
    static constexpr std::string_view null_suffix = ".null";
    if (metadata->getColumns().tryGet(column_name) || !column_name.ends_with(null_suffix))
        return column_name;

    String parent_name = column_name.substr(0, column_name.size() - null_suffix.size());
    const auto * parent_column = metadata->getColumns().tryGet(parent_name);
    if (!parent_column || !isNullableOrLowCardinalityNullable(parent_column->type))
        return column_name;

    const auto * nullable_type = typeid_cast<const DataTypeNullable *>(removeLowCardinality(parent_column->type).get());
    if (nullable_type && !nullable_type->getNestedType()->hasSubcolumn("null"))
        return parent_name;

    return column_name;
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
        index_columns_set.insert(normalizeStatisticsColumnName(col, metadata));

    NameSet raw_filter_input_columns;
    collectFilterInputColumns(filter_node, raw_filter_input_columns);

    NameSet filter_input_columns;
    for (const auto & column_name : raw_filter_input_columns)
        filter_input_columns.insert(normalizeStatisticsColumnName(column_name, metadata));

    Names required_statistics_columns(filter_input_columns.begin(), filter_input_columns.end());
    if (required_statistics_columns.empty())
        return false;

    for (const auto & col : filter_input_columns)
        if (!index_columns_set.contains(col))
            return false;

    ConditionSelectivityEstimatorBuilder builder(context);
    for (const auto & part : parts)
    {
        if (!part.data_part)
            return false;

        if (part.data_part->isEmpty())
        {
            builder.addDataPartStatistics(part.data_part, {});
            continue;
        }

        auto stats = part.data_part->loadStatistics(required_statistics_columns);
        for (const auto & column_name : required_statistics_columns)
        {
            auto it = stats.find(column_name);
            if (it == stats.end() || !it->second)
                return false;
        }

        builder.addDataPartStatistics(part.data_part, stats);
    }

    auto estimator = builder.getEstimator();
    if (!estimator)
        return false;

    if (!estimator->hasStatisticsFor(metadata, filter_input_columns))
        return false;

    if (!estimator->canEstimateFilter(metadata, filter_node))
        return false;

    auto profile = estimator->estimateRelationProfile(metadata, filter_node);
    auto unfiltered = estimator->estimateRelationProfile();
    if (unfiltered.rows == 0)
        return false;

    /// Row-level selectivity as upper bound for granule-level skip ratio
    double selectivity = std::min(1.0, static_cast<double>(profile.rows) / static_cast<double>(unfiltered.rows));
    result.skip_ratio = 1.0 - selectivity;
    result.estimated_marks = std::max<UInt64>(1, static_cast<UInt64>(static_cast<double>(analysis.selected_marks) * selectivity));
    result.estimate_source = WhatIfCandidateResult::Statistical;
    return true;
}

}
