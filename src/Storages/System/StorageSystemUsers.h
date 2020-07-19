#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `users` system table, which allows you to get information about users.
class StorageSystemUsers final : public ext::shared_ptr_helper<StorageSystemUsers>, public IStorageSystemOneBlock<StorageSystemUsers>
{
public:
    std::string getName() const override { return "SystemUsers"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    friend struct ext::shared_ptr_helper<StorageSystemUsers>;
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const override;
};

}
