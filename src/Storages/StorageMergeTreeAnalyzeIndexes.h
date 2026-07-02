#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/VectorSearchUtils.h>
#include <Storages/StorageWithCommonVirtualColumns.h>

namespace DB
{

/// Internal temporary storage for table function mergeTreeAnalyzeIndexes(...)
class StorageMergeTreeAnalyzeIndexes final : public StorageWithCommonVirtualColumns
{
public:
    StorageMergeTreeAnalyzeIndexes(
        const StorageID & table_id_,
        const StoragePtr & source_table_,
        const ColumnsDescription & columns,
        std::vector<String> parts_,
        VectorWithMemoryTracking<MarkRanges> parts_ranges_,
        const ASTPtr & primary_key_predicate_,
        const OptionalVectorSearchParameters & vector_search_parameters_);

    void readImpl(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processing_stage,
        size_t max_block_size,
        size_t num_streams) override;

    String getName() const override { return "MergeTreeAnalyzeIndexes"; }

    static VirtualColumnsDescription createVirtuals();

private:
    friend class ReadFromMergeTreeAnalyzeIndexes;

    StoragePtr source_table;
    MergeTreeData::DataPartsVector data_parts;
    MergeTreeSettingsPtr table_settings;
    ASTPtr predicate;
    OptionalVectorSearchParameters vector_search_parameters;
    /// Absolute mark ranges to analyze per part; parts without an entry are analyzed whole.
    std::unordered_map<String, MarkRanges> requested_ranges;
};

}
