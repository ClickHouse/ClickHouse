#pragma once

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{


class StorageSystemViewRefreshes final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemViewRefreshes"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
