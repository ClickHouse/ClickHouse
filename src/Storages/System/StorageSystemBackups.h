#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

/// Implements `grants` system table, which allows you to get information about grants.
class StorageSystemBackups final : public IStorageSystemOneBlock<StorageSystemBackups>
{
public:
    std::string getName() const override { return "SystemBackups"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
