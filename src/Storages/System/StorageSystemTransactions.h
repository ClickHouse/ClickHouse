#pragma once
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

class StorageSystemTransactions final : public IStorageSystemOneBlock<StorageSystemTransactions>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemTransactions> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemTransactions>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemTransactions(CreatePasskey, TArgs &&... args) : StorageSystemTransactions{std::forward<TArgs>(args)...}
    {
    }

    String getName() const override { return "SystemTransactions"; }

    static NamesAndTypesList getNamesAndTypes();

    static NamesAndAliases getNamesAndAliases() { return {}; }

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
