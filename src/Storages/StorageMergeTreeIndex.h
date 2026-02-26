#pragma once

#include <QueryPipeline/Pipe.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

/// Internal temporary storage for table function mergeTreeIndex(...)
class StorageMergeTreeIndex final : public IStorage
{
public:
    static const ColumnWithTypeAndName part_name_column;
    static const ColumnWithTypeAndName mark_number_column;
    static const ColumnWithTypeAndName rows_in_granule_column;
    static const Block virtuals_sample_block;

    StorageMergeTreeIndex(
        const StorageID & table_id_,
        const StoragePtr & source_table_,
        const ColumnsDescription & columns,
        bool with_marks_);

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processing_stage,
        size_t max_block_size,
        size_t num_streams) override;

    String getName() const override { return "MergeTreeIndex"; }

private:
    friend class ReadFromMergeTreeIndex;

    MergeTreeData::DataPartsVector getFilteredDataParts(const ExpressionActionsPtr & virtual_columns_filter) const;

    StoragePtr source_table;
    bool with_marks;

    MergeTreeData::DataPartsVector data_parts;
    Block key_sample_block;
};

}
