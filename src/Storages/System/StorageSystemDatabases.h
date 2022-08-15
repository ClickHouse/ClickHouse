#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/** Implements `databases` system table, which allows you to get information about all databases.
  */
class StorageSystemDatabases final : public shared_ptr_helper<StorageSystemDatabases>, public IStorageSystemOneBlock<StorageSystemDatabases>
{
    friend struct shared_ptr_helper<StorageSystemDatabases>;
public:
    std::string getName() const override
    {
        return "SystemDatabases";
    }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
