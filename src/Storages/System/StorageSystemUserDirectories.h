#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `users_directories` system table, which allows you to get information about user directories.
class StorageSystemUserDirectories final : public ext::shared_ptr_helper<StorageSystemUserDirectories>, public IStorageSystemOneBlock<StorageSystemUserDirectories>
{
public:
    std::string getName() const override { return "SystemUserDirectories"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    friend struct ext::shared_ptr_helper<StorageSystemUserDirectories>;
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const override;
};

}
