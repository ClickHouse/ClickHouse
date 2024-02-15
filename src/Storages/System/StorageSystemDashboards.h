#pragma once

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


class StorageSystemDashboards final : public IStorageSystemOneBlock<StorageSystemDashboards>
{
public:
    std::string getName() const override { return "SystemDashboards"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
