#pragma once

#include <Storages/StorageWithCommonVirtualColumns.h>


namespace DB
{

class Context;

/// system.cas_mounts: one row per CAS mount slot (`gc/server-roots/<server_root_id>/mount`)
/// visible from every content-addressed disk — live lease state for operators (who holds which
/// slot, renewal health, fenced/terminated). Read-only, one LIST + one GET per slot per disk.
/// The `is_leader` column is per-disk and supersedes the retired process-global `CasGcIsLeader` metric.
class StorageSystemContentAddressedMounts final : public StorageWithCommonVirtualColumns
{
public:
    explicit StorageSystemContentAddressedMounts(const StorageID & table_id_);

    std::string getName() const override { return "SystemContentAddressedMounts"; }

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
