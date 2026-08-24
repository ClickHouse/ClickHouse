#pragma once

#include <Storages/StorageWithCommonVirtualColumns.h>


namespace DB
{

class Context;


/** Implements `zookeeper` system table, which allows you to view the data in ZooKeeper for debugging purposes.
  */
class StorageSystemZooKeeper final : public StorageWithCommonVirtualColumns
{
public:
    explicit StorageSystemZooKeeper(const StorageID & table_id_);

    std::string getName() const override { return "SystemZooKeeper"; }

    static ColumnsDescription getColumnsDescription();
    static VirtualColumnsDescription createVirtuals();

    SinkToStoragePtr write(const ASTPtr & /*query*/, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr /*context*/, bool /*async_insert*/) override;

    void readImpl(
        QueryPlan & query_plan,
        const Names & /*column_names*/,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        size_t /*num_streams*/) override;

    bool isSystemStorage() const override { return true; }
};

}
