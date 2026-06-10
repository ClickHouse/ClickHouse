#pragma once

#include <Storages/StorageWithCommonVirtualColumns.h>

namespace DB
{

class Context;

/** Implements the system table `plain_rewritable_data_paths`, which exposes the
  * local-to-remote path mapping of every `plain_rewritable` disk, as known to the
  * in-memory metadata tree. Unlike `system.remote_data_paths`, which walks the local
  * `store`/`data`/`shadow` namespace, this table enumerates the metadata tree directly,
  * so it also surfaces directories that are not reachable through that namespace.
  */
class StorageSystemPlainRewritableDataPaths final : public StorageWithCommonVirtualColumns
{
public:
    explicit StorageSystemPlainRewritableDataPaths(const StorageID & table_id_);

    std::string getName() const override { return "SystemPlainRewritableDataPaths"; }

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
