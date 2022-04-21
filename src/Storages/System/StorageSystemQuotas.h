#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/** Implements the `quotas` system tables, which allows you to get information about quotas.
  */
class StorageSystemQuotas final : public IStorageSystemOneBlock<StorageSystemQuotas>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemQuotas> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemQuotas>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemQuotas(CreatePasskey, TArgs &&... args) : StorageSystemQuotas{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemQuotas"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
