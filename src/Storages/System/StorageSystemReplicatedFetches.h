#pragma once


#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

/// system.replicated_fetches table. Takes data from context.getReplicatedFetchList()
class StorageSystemReplicatedFetches final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemReplicatedFetches"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
