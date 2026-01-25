#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
    extern const Event HybridStorageRowBasedReads;
    extern const Event HybridStorageColumnBasedReads;
}

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool enable_hybrid_storage;
    extern const MergeTreeSettingsFloat hybrid_storage_column_threshold;
    extern const MergeTreeSettingsUInt64 hybrid_storage_min_columns;
}

namespace QueryPlanOptimizations
{

namespace
{

/// Analyzes the cost of reading with hybrid storage vs traditional column-based reading.
struct HybridStorageCostAnalysis
{
    bool prefer_row_based = false;
    size_t total_columns = 0;
    size_t requested_columns = 0;
    size_t key_columns = 0;
    float selection_ratio = 0.0f;
    float threshold = 0.5f;
    size_t min_columns = 0;
};

HybridStorageCostAnalysis analyzeHybridStorageCost(
    const MergeTreeData & storage,
    const Names & columns_to_read,
    const MergeTreeSettingsPtr & settings)
{
    HybridStorageCostAnalysis result;

    const auto & metadata = storage.getInMemoryMetadataPtr();
    const auto & all_columns = metadata->getColumns().getAllPhysical();

    result.total_columns = all_columns.size();
    result.requested_columns = columns_to_read.size();

    /// Exclude key columns from calculation since they are always needed
    /// for sorting/filtering and are read separately
    const auto & primary_key = metadata->getPrimaryKey();
    result.key_columns = primary_key.column_names.size();

    size_t non_key_columns = result.total_columns > result.key_columns
        ? result.total_columns - result.key_columns
        : 0;

    if (non_key_columns == 0)
        return result;

    /// Count how many of the requested columns are non-key columns
    std::unordered_set<String> key_column_set(
        primary_key.column_names.begin(),
        primary_key.column_names.end());

    size_t requested_non_key_columns = 0;
    for (const auto & col : columns_to_read)
    {
        if (key_column_set.find(col) == key_column_set.end())
            ++requested_non_key_columns;
    }

    /// Calculate selection ratio
    result.selection_ratio = static_cast<float>(requested_non_key_columns) / static_cast<float>(non_key_columns);
    result.threshold = (*settings)[MergeTreeSetting::hybrid_storage_column_threshold];
    result.min_columns = (*settings)[MergeTreeSetting::hybrid_storage_min_columns];

    /// Prefer row-based reading if:
    /// 1. Selection ratio exceeds threshold (reading many columns relative to total)
    /// 2. AND if min_columns is set, we're reading at least that many columns
    bool exceeds_threshold = result.selection_ratio >= result.threshold;
    bool meets_min_columns = (result.min_columns == 0) || (result.requested_columns >= result.min_columns);

    result.prefer_row_based = exceeds_threshold && meets_min_columns;

    return result;
}

}

/// Optimization pass for hybrid row-column storage.
/// This optimization analyzes ReadFromMergeTree steps and determines whether
/// to use row-based reading (from __row column) instead of traditional
/// column-based reading when it's more efficient.
///
/// The decision is based on the ratio of requested columns to total columns:
/// - If we're reading a large fraction of columns (above threshold), use row-based reading
/// - Otherwise, use traditional column-based reading
size_t tryOptimizeHybridStorage(QueryPlan::Node * parent_node, QueryPlan::Nodes & /*nodes*/, const Optimization::ExtraSettings & /*settings*/)
{
    if (!parent_node)
        return 0;

    auto * reading_step = typeid_cast<ReadFromMergeTree *>(parent_node->step.get());
    if (!reading_step)
        return 0;

    const auto & storage = reading_step->getMergeTreeData();
    const auto & storage_settings = storage.getSettings();

    /// Check if hybrid storage is enabled for this table
    if (!(*storage_settings)[MergeTreeSetting::enable_hybrid_storage])
        return 0;

    /// Skip if already using hybrid row reading
    if (reading_step->isUsingHybridRowReading())
        return 0;

    /// Get the columns being read
    const auto & columns_to_read = reading_step->getAllColumnNames();

    /// Analyze cost
    auto cost_analysis = analyzeHybridStorageCost(storage, columns_to_read, storage_settings);

    if (cost_analysis.prefer_row_based)
    {
        /// Enable hybrid row-based reading
        reading_step->enableHybridRowReading();

        /// Track that row-based reading was chosen
        ProfileEvents::increment(ProfileEvents::HybridStorageRowBasedReads);

        LOG_DEBUG(
            getLogger("QueryPlanOptimizations"),
            "Enabled hybrid row-based reading for table {}. "
            "Columns requested: {}/{} (non-key: {:.1f}%), threshold: {:.0f}%, min_columns: {}",
            storage.getStorageID().getNameForLogs(),
            cost_analysis.requested_columns,
            cost_analysis.total_columns,
            cost_analysis.selection_ratio * 100.0f,
            cost_analysis.threshold * 100.0f,
            cost_analysis.min_columns);

        /// Return 1 to indicate the optimization was applied
        /// (no tree structure changes, just internal state change)
        return 1;
    }

    /// Track that column-based reading was chosen (when hybrid storage is enabled)
    ProfileEvents::increment(ProfileEvents::HybridStorageColumnBasedReads);

    return 0;
}

}

}
