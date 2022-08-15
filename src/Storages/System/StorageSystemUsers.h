#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `users` system table, which allows you to get information about users.
class StorageSystemUsers final : public shared_ptr_helper<StorageSystemUsers>, public IStorageSystemOneBlock<StorageSystemUsers>
{
public:
    std::string getName() const override { return "SystemUsers"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    friend struct shared_ptr_helper<StorageSystemUsers>;
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
