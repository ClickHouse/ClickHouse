#pragma once

#include "config.h"

#include <Storages/StorageWithCommonVirtualColumns.h>

namespace DB
{

/// System table that flushes a jemalloc heap profile and exposes one row per live sampled allocation
class StorageSystemJemallocSampledAllocations final : public StorageWithCommonVirtualColumns
{
public:
    explicit StorageSystemJemallocSampledAllocations(const StorageID & table_id_);

    std::string getName() const override { return "SystemJemallocSampledAllocations"; }

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

    bool supportsTransactions() const override { return true; }

#if USE_JEMALLOC
    /// Rows of the heap profile in `profile_path`; the file is removed when the source is destroyed.
    static Pipe readHeapProfile(std::string profile_path, SharedHeader header, size_t max_block_size);
#endif
};

}
