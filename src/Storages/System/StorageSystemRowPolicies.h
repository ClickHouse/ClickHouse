#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/// Implements `row_policies` system table, which allows you to get information about row policies.
class StorageSystemRowPolicies final : public IStorageSystemOneBlock<StorageSystemRowPolicies>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemRowPolicies> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemRowPolicies>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemRowPolicies(CreatePasskey, TArgs &&... args) : StorageSystemRowPolicies{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemRowPolicies"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
