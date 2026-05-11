#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

/// system.live_source_buffers — shows all active live source connections held by ReaderExecutor.
class StorageSystemLiveSourceBuffers final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemLiveSourceBuffers"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
