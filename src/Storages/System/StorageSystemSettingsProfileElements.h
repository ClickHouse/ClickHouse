#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `settings_profile_elements` system table, which allows you to get information about elements of settings profiles.
class StorageSystemSettingsProfileElements final : public IStorageSystemOneBlock<StorageSystemSettingsProfileElements>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemSettingsProfileElements> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemSettingsProfileElements>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemSettingsProfileElements(CreatePasskey, TArgs &&... args) : StorageSystemSettingsProfileElements{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemSettingsProfileElements"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
