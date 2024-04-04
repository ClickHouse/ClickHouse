#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements system table "settings_changes", which allows to get information
  * about the settings changes through different ClickHouse versions.
  */
class StorageSystemSettingsChanges final : public IStorageSystemOneBlock<StorageSystemSettingsChanges>
{
public:
    std::string getName() const override { return "SystemSettingsChanges"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
