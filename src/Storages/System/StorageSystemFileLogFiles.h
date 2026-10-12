#pragma once

#include "config.h"

#if USE_FILELOG

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class StorageSystemFileLogFiles final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemFileLogFiles"; }
    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}

#endif
