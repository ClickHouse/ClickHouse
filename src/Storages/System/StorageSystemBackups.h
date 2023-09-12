#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

/// Implements `backups` system table, which allows you to get information about backup and restore operations.
class StorageSystemBackups final : public IStorageSystemOneBlock<StorageSystemBackups>
{
public:
    static constexpr const char* ENGINE_NAME = "SystemBackups";
    std::string getName() const override { return ENGINE_NAME; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;
};

}
