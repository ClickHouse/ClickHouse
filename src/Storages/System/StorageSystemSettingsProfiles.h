#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `settings_profiles` system table, which allows you to get information about profiles.
class StorageSystemSettingsProfiles final : public ext::shared_ptr_helper<StorageSystemSettingsProfiles>, public IStorageSystemOneBlock<StorageSystemSettingsProfiles>
{
public:
    std::string getName() const override { return "SystemSettingsProfiles"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    friend struct ext::shared_ptr_helper<StorageSystemSettingsProfiles>;
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const override;
};

}
