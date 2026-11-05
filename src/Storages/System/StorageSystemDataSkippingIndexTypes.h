#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

/// Provides embedded documentation for the data skipping index types supported by the server.
class StorageSystemDataSkippingIndexTypes final : public IStorageSystemOneBlock
{
protected:
    void fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override { return "SystemDataSkippingIndexTypes"; }

    static ColumnsDescription getColumnsDescription();
};

}
