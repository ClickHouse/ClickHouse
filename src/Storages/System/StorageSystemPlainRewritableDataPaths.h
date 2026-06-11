#pragma once

#include <Storages/StorageWithCommonVirtualColumns.h>

namespace DB
{

class Context;

/** Implements the system table `plain_rewritable_data_paths`, which exposes the
  * local-to-remote path mapping of every `plain_rewritable` disk, like `system.remote_data_paths`
  * does for disks with local per-file metadata. The difference is the disk `metadata_type`:
  * `plain_rewritable` disks keep their metadata in memory, so this table reads that in-memory
  * metadata tree directly instead of walking the local `store`/`data`/`shadow` namespace.
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
