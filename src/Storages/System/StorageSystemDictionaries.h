#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


class StorageSystemDictionaries final : public IStorageSystemOneBlock<StorageSystemDictionaries>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemDictionaries> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemDictionaries>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemDictionaries(CreatePasskey, TArgs &&... args) : StorageSystemDictionaries{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemDictionaries"; }

    static NamesAndTypesList getNamesAndTypes();

    NamesAndTypesList getVirtuals() const override;

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
