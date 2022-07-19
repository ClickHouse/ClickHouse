#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `settings_profile_elements` system table, which allows you to get information about elements of settings profiles.
class StorageSystemSettingsProfileElements final : public shared_ptr_helper<StorageSystemSettingsProfileElements>, public IStorageSystemOneBlock<StorageSystemSettingsProfileElements>
{
public:
    std::string getName() const override { return "SystemSettingsProfileElements"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    friend struct shared_ptr_helper<StorageSystemSettingsProfileElements>;
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
