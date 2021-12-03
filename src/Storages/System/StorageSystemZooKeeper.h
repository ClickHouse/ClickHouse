#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements `zookeeper` system table, which allows you to view the data in ZooKeeper for debugging purposes.
  */
class StorageSystemZooKeeper final : public ext::shared_ptr_helper<StorageSystemZooKeeper>, public IStorageSystemOneBlock<StorageSystemZooKeeper>
{
    friend struct ext::shared_ptr_helper<StorageSystemZooKeeper>;
public:
    std::string getName() const override { return "SystemZooKeeper"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo & query_info) const override;
};

}
