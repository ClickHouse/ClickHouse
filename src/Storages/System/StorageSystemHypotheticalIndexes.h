#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

/// system.hypothetical_indexes -- shows session-scoped hypothetical indexes.
class StorageSystemHypotheticalIndexes final : public IStorageSystemOneBlock
{
protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override { return "SystemHypotheticalIndexes"; }

    static ColumnsDescription getColumnsDescription();
};

}
