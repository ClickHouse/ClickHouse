#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `settings_profile_elements` system table, which allows you to get information about elements of settings profiles.
class StorageSystemSettingsProfileElements final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemSettingsProfileElements"; }
    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
