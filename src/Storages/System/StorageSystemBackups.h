#pragma once

#include <base/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

/// Implements `grants` system table, which allows you to get information about grants.
class StorageSystemBackups final : public shared_ptr_helper<StorageSystemBackups>, public IStorageSystemOneBlock<StorageSystemBackups>
{
public:
    std::string getName() const override { return "SystemBackups"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    friend struct shared_ptr_helper<StorageSystemBackups>;
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
