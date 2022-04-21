#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{
class StorageSystemFormats final : public IStorageSystemOneBlock<StorageSystemFormats>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemFormats> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemFormats>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemFormats(CreatePasskey, TArgs &&... args) : StorageSystemFormats{std::forward<TArgs>(args)...}
    {
    }

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override
    {
        return "SystemFormats";
    }

    static NamesAndTypesList getNamesAndTypes();
};
}
