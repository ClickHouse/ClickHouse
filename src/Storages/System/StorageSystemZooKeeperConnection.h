#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements `zookeeper_connection` system table, which allows you to get information about the connected zookeeper info.
  */
class StorageSystemZooKeeperConnection final : public IStorageSystemOneBlock<StorageSystemZooKeeperConnection>
{
public:
    std::string getName() const override { return "SystemZooKeeperConnection"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}

