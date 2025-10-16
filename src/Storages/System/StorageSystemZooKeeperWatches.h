#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements `zookeeper_watches` system table.
  * Shows active ZooKeeper watches created by ClickHouse server.
  * Useful for debugging watch leaks and monitoring ZooKeeper usage.
  */
class StorageSystemZooKeeperWatches final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemZooKeeperWatches"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
