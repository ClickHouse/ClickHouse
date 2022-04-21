#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `settings_profiles` system table, which allows you to get information about profiles.
class StorageSystemSettingsProfiles final : public IStorageSystemOneBlock<StorageSystemSettingsProfiles>
{
private:
    struct CreatePasskey
    {
    };

public:
    template <typename... TArgs>
    static std::shared_ptr<StorageSystemSettingsProfiles> create(TArgs &&... args)
    {
        return std::make_shared<StorageSystemSettingsProfiles>(CreatePasskey{}, std::forward<TArgs>(args)...);
    }

    template <typename... TArgs>
    explicit StorageSystemSettingsProfiles(CreatePasskey, TArgs &&... args) : StorageSystemSettingsProfiles{std::forward<TArgs>(args)...}
    {
    }

    std::string getName() const override { return "SystemSettingsProfiles"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
