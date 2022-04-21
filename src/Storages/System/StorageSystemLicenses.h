#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;


/** System table "licenses" with list of licenses of 3rd party libraries
  */
class StorageSystemLicenses final : public IStorageSystemOneBlock<StorageSystemLicenses>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemLicenses> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemLicenses>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemLicenses(CreatePasskey, TArgs &&... args) : StorageSystemLicenses{std::forward<TArgs>(args)...}
    {
    }

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override
    {
        return "SystemLicenses";
    }

    static NamesAndTypesList getNamesAndTypes();
};
}
