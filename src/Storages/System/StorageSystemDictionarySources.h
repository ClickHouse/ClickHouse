#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

/// Provides embedded documentation for the dictionary sources supported by the server.
class StorageSystemDictionarySources final : public IStorageSystemOneBlock
{
protected:
    void fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override { return "SystemDictionarySources"; }

    static ColumnsDescription getColumnsDescription();
};

}
