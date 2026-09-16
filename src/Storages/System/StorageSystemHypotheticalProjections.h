#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

/// system.hypothetical_projections — session-scoped hypothetical projections
class StorageSystemHypotheticalProjections final : public IStorageSystemOneBlock
{
protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override { return "SystemHypotheticalProjections"; }

    static ColumnsDescription getColumnsDescription();
};

}
