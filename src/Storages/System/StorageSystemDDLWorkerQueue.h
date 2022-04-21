#pragma once
#include <Interpreters/DDLWorker.h>
#include <Storages/System/IStorageSystemOneBlock.h>
#include <future>

namespace DB
{
class Context;


/** System table "distributed_ddl_queue" with list of queries that are currently in the DDL worker queue.
  */
class StorageSystemDDLWorkerQueue final : public IStorageSystemOneBlock<StorageSystemDDLWorkerQueue>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemDDLWorkerQueue> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemDDLWorkerQueue>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemDDLWorkerQueue(CreatePasskey, TArgs &&... args) : StorageSystemDDLWorkerQueue{std::forward<TArgs>(args)...}
    {
    }

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override { return "SystemDDLWorkerQueue"; }

    static NamesAndTypesList getNamesAndTypes();
};
}
