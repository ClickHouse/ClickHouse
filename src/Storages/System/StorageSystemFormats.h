#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{
class StorageSystemFormats final : public IStorageSystemOneBlock
{
protected:
    void fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override
    {
        return "SystemFormats";
    }

    static ColumnsDescription getColumnsDescription();
};
}
