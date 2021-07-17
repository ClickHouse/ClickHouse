#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `current_roles` system table, which allows you to get information about current roles.
class StorageSystemCurrentRoles final : public shared_ptr_helper<StorageSystemCurrentRoles>, public IStorageSystemOneBlock<StorageSystemCurrentRoles>
{
public:
    std::string getName() const override { return "SystemCurrentRoles"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    friend struct shared_ptr_helper<StorageSystemCurrentRoles>;
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
