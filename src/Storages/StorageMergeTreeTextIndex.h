#pragma once

#include <QueryPipeline/Pipe.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/StorageWithCommonVirtualColumns.h>

namespace DB
{

/// Internal temporary storage for table function mergeTreeTextIndex(...)
class StorageMergeTreeTextIndex final : public StorageWithCommonVirtualColumns
{
public:
    static const ColumnWithTypeAndName part_name_column;

    StorageMergeTreeTextIndex(
        const StorageID & table_id_,
        const StoragePtr & source_table_,
        StorageMetadataPtr source_metadata_,
        MergeTreeIndexPtr text_index_,
        const ColumnsDescription & columns);

    void readImpl(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processing_stage,
        size_t max_block_size,
        size_t num_streams) override;

    String getName() const override { return "MergeTreeTextIndex"; }

    static VirtualColumnsDescription createVirtuals();

    /// Throws if the user may not read the tokens of `index` of `source_table`. `source_metadata` must be the metadata
    /// the index was built from, so the columns the check authorizes are the ones the index is computed over.
    static void checkAccess(
        const ContextPtr & context, const IStorage & source_table, const StorageInMemoryMetadata & source_metadata, const IMergeTreeIndex & index);

private:
    friend class ReadFromMergeTreeTextIndex;

    StoragePtr source_table;
    /// The source metadata `text_index` was built from.
    StorageMetadataPtr source_metadata;
    MergeTreeIndexPtr text_index;
};

}
