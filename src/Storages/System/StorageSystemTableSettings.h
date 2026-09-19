#pragma once

#include <Storages/StorageWithCommonVirtualColumns.h>

namespace DB
{

/// Settings of every table, as they are actually in effect - see `IStorage::getTableSettings`.
///
/// `system.engine_settings` describes engines; this describes tables. A setting appears here with
/// the value its table uses, which need not be the value its `CREATE` query states.
class StorageSystemTableSettings final : public StorageWithCommonVirtualColumns
{
public:
    explicit StorageSystemTableSettings(const StorageID & table_id_);

    std::string getName() const override { return "SystemTableSettings"; }

    void readImpl(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    bool isSystemStorage() const override { return true; }

    static ColumnsDescription getColumnsDescription();
    static VirtualColumnsDescription createVirtuals();
};

}
