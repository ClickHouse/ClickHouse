#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include "Storages/ColumnsDescription.h"

namespace DB
{
class StorageSystemUnicode final : public IStorageSystemOneBlock
{
protected:
    void fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const override;
    Block getFilterSampleBlock() const override;
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
public:
    std::string getName() const override
    {
        return "SystemUnicode";
    }

    static ColumnsDescription getColumnsDescription();
};

}
