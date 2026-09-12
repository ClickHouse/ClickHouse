#pragma once

#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

/// Backing storage for `mergeTreeCodecBlockCounts(database, table)`. One row per (part, column, substream).
/// Counts compressed blocks per codec by reading each stream's `.bin` header. Selecting `part_name`/`column`/`substream` is metadata-only.
class StorageMergeTreeCodecBlockCounts final : public IStorage
{
public:
    StorageMergeTreeCodecBlockCounts(const StorageID & table_id_, StoragePtr source_table_, const ColumnsDescription & columns_);

    std::string getName() const override { return "MergeTreeCodecBlockCounts"; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

private:
    StoragePtr source_table;
    MergeTreeData::DataPartsVector data_parts;
};

}
