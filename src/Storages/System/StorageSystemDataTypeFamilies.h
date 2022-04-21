#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class StorageSystemDataTypeFamilies final : public IStorageSystemOneBlock<StorageSystemDataTypeFamilies>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemDataTypeFamilies> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemDataTypeFamilies>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemDataTypeFamilies(CreatePasskey, TArgs &&... args) : StorageSystemDataTypeFamilies{std::forward<TArgs>(args)...}
    {
    }

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override { return "SystemTableDataTypeFamilies"; }

    static NamesAndTypesList getNamesAndTypes();
};

}
