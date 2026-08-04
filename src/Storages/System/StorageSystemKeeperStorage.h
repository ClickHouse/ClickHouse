#pragma once

#include "config.h"

#if USE_NURAFT

#include <Storages/StorageWithCommonVirtualColumns.h>

namespace DB
{

/** Implements `keeper_storage` system table, which exposes the data tree of the local
  * ClickHouse Keeper through a lock-free MVCC-style read view.
  */
class StorageSystemKeeperStorage final : public StorageWithCommonVirtualColumns
{
public:
    explicit StorageSystemKeeperStorage(const StorageID & table_id_);

    std::string getName() const override { return "SystemKeeperStorage"; }

    static ColumnsDescription getColumnsDescription();
    static VirtualColumnsDescription createVirtuals();

    using StorageWithCommonVirtualColumns::read;

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    bool isSystemStorage() const override { return true; }
};

}

#endif
