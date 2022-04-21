#pragma once
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class StorageSystemCollations final : public IStorageSystemOneBlock<StorageSystemCollations>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemCollations> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemCollations>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemCollations(CreatePasskey, TArgs &&... args) : StorageSystemCollations{std::forward<TArgs>(args)...}
    {
    }

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;
public:

    std::string getName() const override { return "SystemTableCollations"; }

    static NamesAndTypesList getNamesAndTypes();
};

}
