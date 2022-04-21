#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements the `distribution_queue` system table, which allows you to view the INSERT queues for the Distributed tables.
  */
class StorageSystemDistributionQueue final : public IStorageSystemOneBlock<StorageSystemDistributionQueue>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemDistributionQueue> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemDistributionQueue>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemDistributionQueue(CreatePasskey, TArgs &&... args) : StorageSystemDistributionQueue{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemDistributionQueue"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
