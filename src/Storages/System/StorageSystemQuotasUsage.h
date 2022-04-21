#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/** Implements the `quotas_usage` system table, which allows you to get information about
  * how all users use the quotas.
  */
class StorageSystemQuotasUsage final : public IStorageSystemOneBlock<StorageSystemQuotasUsage>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemQuotasUsage> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemQuotasUsage>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemQuotasUsage(CreatePasskey, TArgs &&... args) : StorageSystemQuotasUsage{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemQuotasUsage"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
