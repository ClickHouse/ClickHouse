#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements `zookeeper` system table, which allows you to view the data in ZooKeeper for debugging purposes.
  */
class StorageSystemZooKeeper final : public IStorage
{
public:
    explicit StorageSystemZooKeeper(const StorageID & table_id_);

    std::string getName() const override { return "SystemZooKeeper"; }

    static NamesAndTypesList getNamesAndTypes();

    SinkToStoragePtr write(const ASTPtr & /*query*/, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr /*context*/, bool /*async_insert*/) override;

    void read(
        QueryPlan & query_plan,
        const Names & /*column_names*/,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        size_t /*num_streams*/) override;

    bool isSystemStorage() const override { return true; }
    bool supportsIndexForIn() const override { return true; }
    bool mayBenefitFromIndexForIn(const ASTPtr & node, ContextPtr, const StorageMetadataPtr &) const override;
};

}
