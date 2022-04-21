#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** implements system table "settings", which allows to get information about the current settings.
  */
class StorageSystemSettings final : public IStorageSystemOneBlock<StorageSystemSettings>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemSettings> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemSettings>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemSettings(CreatePasskey, TArgs &&... args) : StorageSystemSettings{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemSettings"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
