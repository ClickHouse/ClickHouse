#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements `zookeeper_connection` system table, which allows you to get information about the connected zookeeper info.
  */
class StorageSystemZooKeeperConnection final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemZooKeeperConnection"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
