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

    /// Every column of this function is derived from the source table's data, so reading any of them requires
    /// `SELECT` on all of the source table's columns. Called both when the function's structure is resolved and
    /// when it is read, so that resolving the structure cannot reveal anything about a table the user cannot select from.
    static void checkSourceTableAccess(const StoragePtr & source_table, const ContextPtr & context);

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
