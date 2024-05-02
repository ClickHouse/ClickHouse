#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements system table "settings_changes", which allows to get information
  * about the settings changes through different ClickHouse versions.
  */
class StorageSystemSettingsChanges final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemSettingsChanges"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
