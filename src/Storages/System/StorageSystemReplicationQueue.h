#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements the `replication_queue` system table, which allows you to view the replication queues for the replicated tables.
  */
class StorageSystemReplicationQueue final : public IStorageSystemOneBlock<StorageSystemReplicationQueue>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemReplicationQueue> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemReplicationQueue>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemReplicationQueue(CreatePasskey, TArgs &&... args) : StorageSystemReplicationQueue{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemReplicationQueue"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
