#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


class StorageSystemModels final : public IStorageSystemOneBlock<StorageSystemModels>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemModels> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemModels>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemModels(CreatePasskey, TArgs &&... args) : StorageSystemModels{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemModels"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
