#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `users` system table, which allows you to get information about users.
class StorageSystemUsers final : public IStorageSystemOneBlock<StorageSystemUsers>
{
public:
    std::string getName() const override { return "SystemUsers"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
