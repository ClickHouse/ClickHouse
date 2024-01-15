#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements `zookeeper` system table, which allows you to view the data in ZooKeeper for debugging purposes.
  */
class StorageSystemZooKeeper final : public IStorageSystemOneBlock<StorageSystemZooKeeper>
{
public:
    explicit StorageSystemZooKeeper(const StorageID & table_id_);

    std::string getName() const override { return "SystemZooKeeper"; }

    static NamesAndTypesList getNamesAndTypes();

    SinkToStoragePtr write(const ASTPtr & /*query*/, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr /*context*/) override;

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
