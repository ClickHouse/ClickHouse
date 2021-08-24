#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `privileges` system table, which allows you to get information about access types.
class StorageSystemPrivileges final : public shared_ptr_helper<StorageSystemPrivileges>, public IStorageSystemOneBlock<StorageSystemPrivileges>
{
public:
    std::string getName() const override { return "SystemPrivileges"; }
    static NamesAndTypesList getNamesAndTypes();
    static const std::vector<std::pair<String, Int8>> & getAccessTypeEnumValues();

protected:
    friend struct shared_ptr_helper<StorageSystemPrivileges>;
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
