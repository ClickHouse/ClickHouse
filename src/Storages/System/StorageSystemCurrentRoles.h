#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `current_roles` system table, which allows you to get information about current roles.
class StorageSystemCurrentRoles final : public IStorageSystemOneBlock<StorageSystemCurrentRoles>
{
public:
    std::string getName() const override { return "SystemCurrentRoles"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
