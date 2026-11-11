#pragma once

#include <Storages/StorageWithCommonVirtualColumns.h>


namespace DB
{

class Context;

/** System table that lists data and delete files of currently loaded Iceberg tables.
  * Each row corresponds to one file referenced by the current snapshot's manifest entries.
  */
class StorageSystemIcebergFiles final : public StorageWithCommonVirtualColumns
{
public:
    explicit StorageSystemIcebergFiles(const StorageID & table_id_);

    std::string getName() const override { return "SystemIcebergFiles"; }

    bool isSystemStorage() const override { return true; }

protected:
    void readImpl(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;
};

}
