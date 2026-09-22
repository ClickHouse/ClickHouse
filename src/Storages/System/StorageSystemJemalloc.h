#pragma once

#include <Storages/StorageWithCommonVirtualColumns.h>


namespace DB
{

/// Statistics of jemalloc size classes (bins): aggregated over all arenas when
/// `per_arena` is false, with one row per (arena, bin) otherwise.
class StorageSystemJemallocBins final : public StorageWithCommonVirtualColumns
{
public:
    StorageSystemJemallocBins(const StorageID & table_id_, bool per_arena_);

    std::string getName() const override { return per_arena ? "SystemJemallocArenaBins" : "SystemJemallocBins"; }

    static ColumnsDescription getColumnsDescription(bool per_arena);
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

    bool supportsTransactions() const override { return true; }

private:
    const bool per_arena;
};

}
