#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/// Implements the `mutations` system table, which provides information about the status of mutations
/// in the MergeTree tables.
class StorageSystemMutations final : public IStorageSystemOneBlock
{
public:
    String getName() const override { return "SystemMutations"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8>) const override;
    Block getFilterSampleBlock() const override;
};

}
