#pragma once

#include <Storages/ColumnsDescription.h>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{
class StorageSystemUnicode final : public IStorageSystemOneBlock
{
protected:
    void fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const override;
    Block getFilterSampleBlock() const override;
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    bool supportsColumnsMask() const override { return true; }

public:
    std::string getName() const override { return "SystemUnicode"; }

    static ColumnsDescription getColumnsDescription();
};

}
