#pragma once

#include <Storages/IStorage.h>

namespace DB
{

/// Intermediate base class that automatically materializes constant virtual
/// columns (like `_table`) after the storage's own read logic.
///
/// Storages that inherit from this class should override `readImpl` instead of
/// the public `read(QueryPlan&, ...)`.  Storages that only override the private
/// Pipe-based `read(...)` don't need any changes — the default `readImpl`
/// delegates to `IStorage::read(QueryPlan&, ...)` which calls it.
class StorageWithCommonVirtualColumns : public IStorage
{
public:
    using IStorage::IStorage;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

protected:
    virtual void readImpl(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams);
};

}
