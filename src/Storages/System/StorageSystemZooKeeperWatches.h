#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements the `zookeeper_watches` system table, which exposes all active ZooKeeper watches
  * across all connections (default and auxiliary).
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
