#pragma once

#include <DataTypes/DataTypeString.h>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class StorageSystemDatabaseEngines final : public IStorageSystemOneBlock<StorageSystemDatabaseEngines>
{
protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override { return "SystemDatabaseEngines"; }

    static ColumnsDescription getColumnsDescription();
};

}
