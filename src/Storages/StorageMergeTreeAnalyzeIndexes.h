#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
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
        String projection_name_,
        const ASTPtr & primary_key_predicate_,
        const OptionalVectorSearchParameters & vector_search_parameters_,
        ContextPtr context);

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
    StorageMetadataHandle source_metadata_snapshot;
    MergeTreeData::DataPartsVector data_parts;
    /// Parallel to data_parts when projection_name is set; data_parts then keeps the parent
    /// parts (their names identify the projection parts in the result), and parents without
    /// a usable projection part are dropped so that the initiator analyzes them itself.
    MergeTreeData::DataPartsVector projection_parts;
    String projection_name;
    MergeTreeSettingsPtr table_settings;
    ASTPtr predicate;
    OptionalVectorSearchParameters vector_search_parameters;
};

}
