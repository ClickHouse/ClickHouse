#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements the `replication_queue` system table, which allows you to view the replication queues for the replicated tables.
  */
class StorageSystemReplicationQueue final : public ext::shared_ptr_helper<StorageSystemReplicationQueue>, public IStorageSystemOneBlock<StorageSystemReplicationQueue>
{
    friend struct ext::shared_ptr_helper<StorageSystemReplicationQueue>;
public:
    std::string getName() const override { return "SystemReplicationQueue"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
