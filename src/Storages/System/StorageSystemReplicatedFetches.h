#pragma once


#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

/// system.replicated_fetches table. Takes data from context.getReplicatedFetchList()
class StorageSystemReplicatedFetches final : public IStorageSystemOneBlock<StorageSystemReplicatedFetches>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemReplicatedFetches> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemReplicatedFetches>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemReplicatedFetches(CreatePasskey, TArgs &&... args) : StorageSystemReplicatedFetches{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemReplicatedFetches"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
