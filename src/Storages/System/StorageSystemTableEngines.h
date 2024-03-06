#pragma once

#include <DataTypes/DataTypeString.h>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class StorageSystemTableEngines final : public IStorageSystemOneBlock
{
protected:
    void fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override
    {
        return "SystemTableEngines";
    }

    static ColumnsDescription getColumnsDescription();
};

}
