#pragma once

#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

/// Backing storage for `mergeTreeCodecBlockCounts(database, table)`.
/// One row per (part, column); counts compressed blocks per codec by walking `.bin` headers (reading data files).
/// Selecting only `part_name`/`column`/`subcolumns.names` is metadata-only.
class StorageMergeTreeCodecBlockCounts final : public IStorage
{
public:
    StorageMergeTreeCodecBlockCounts(const StorageID & table_id_, StoragePtr source_table_, const ColumnsDescription & columns_);

    std::string getName() const override { return "MergeTreeCodecBlockCounts"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

private:
    StoragePtr source_table;
    MergeTreeSettingsPtr storage_settings;
    MergeTreeData::DataPartsVector data_parts;
};

}
