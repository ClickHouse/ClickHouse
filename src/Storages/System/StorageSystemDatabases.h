#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements `databases` system table, which allows you to get information about all databases.
  */
class StorageSystemDatabases final : public IStorageSystemOneBlock<StorageSystemDatabases>
{
public:
    std::string getName() const override
    {
        return "SystemDatabases";
    }

    static NamesAndTypesList getNamesAndTypes();

    static NamesAndAliases getNamesAndAliases();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
