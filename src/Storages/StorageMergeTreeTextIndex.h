#pragma once

#include <QueryPipeline/Pipe.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndices.h>

namespace DB
{

/// Internal temporary storage for table function mergeTreeTextIndex(...)
class StorageMergeTreeTextIndex final : public IStorage
{
public:
    static const ColumnWithTypeAndName part_name_column;

    StorageMergeTreeTextIndex(
        const StorageID & table_id_,
        const StoragePtr & source_table_,
        MergeTreeIndexPtr text_index_,
        const ColumnsDescription & columns);

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processing_stage,
        size_t max_block_size,
        size_t num_streams) override;

    String getName() const override { return "MergeTreeTextIndex"; }

    /// Throws if the user may not read the tokens of the index of the table `source_storage_id`.
    static void checkAccess(const ContextPtr & context, const StorageID & source_storage_id, const IMergeTreeIndex & index);

private:
    friend class ReadFromMergeTreeTextIndex;

    StoragePtr source_table;
    MergeTreeIndexPtr text_index;
};

}
