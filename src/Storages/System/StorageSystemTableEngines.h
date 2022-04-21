#pragma once

#include <DataTypes/DataTypeString.h>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class StorageSystemTableEngines final : public IStorageSystemOneBlock<StorageSystemTableEngines>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemTableEngines> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemTableEngines>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemTableEngines(CreatePasskey, TArgs &&... args) : StorageSystemTableEngines{std::forward<TArgs>(args)...}
    {
    }

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override
    {
        return "SystemTableEngines";
    }

    static NamesAndTypesList getNamesAndTypes();
};

}
