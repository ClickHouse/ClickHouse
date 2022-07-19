#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `roles` system table, which allows you to get information about roles.
class StorageSystemRoles final : public shared_ptr_helper<StorageSystemRoles>, public IStorageSystemOneBlock<StorageSystemRoles>
{
public:
    std::string getName() const override { return "SystemRoles"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    friend struct shared_ptr_helper<StorageSystemRoles>;
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
