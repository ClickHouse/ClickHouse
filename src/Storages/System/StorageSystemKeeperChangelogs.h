#pragma once

#include "config.h"

#if USE_NURAFT

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class StorageSystemKeeperChangelogs final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemKeeperChangelogs"; }
    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}

#endif
