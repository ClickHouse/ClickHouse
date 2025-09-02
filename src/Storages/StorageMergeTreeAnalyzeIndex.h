#pragma once

#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

/// Internal temporary storage for table function mergeTreeAnalyzeIndex(...)
class StorageMergeTreeAnalyzeIndex final : public IStorage
{
public:
    StorageMergeTreeAnalyzeIndex(
        const StorageID & table_id_,
        const StoragePtr & source_table_,
        const ColumnsDescription & columns,
        const String & parts_regexp_,
        const ASTPtr & primary_key_predicate_);

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processing_stage,
        size_t max_block_size,
        size_t num_streams) override;

    String getName() const override { return "MergeTreeAnalyzeIndex"; }

private:
    friend class ReadFromMergeTreeAnalyzeIndex;

    StoragePtr source_table;
    MergeTreeData::DataPartsVector data_parts;
    MergeTreeSettingsPtr table_settings;
    ASTPtr primary_key_predicate;
};

}
