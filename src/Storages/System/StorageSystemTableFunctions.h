#pragma once

#include <DataTypes/DataTypeString.h>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class StorageSystemTableFunctions final : public IStorageSystemOneBlock
{
protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const override;

public:
    std::string getName() const override
    {
        return "SystemTableFunctions";
    }

    static ColumnsDescription getColumnsDescription();
};

}
