#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

/// Implements `system.table_settings`: the engine-specific settings of tables, one row per table and setting.
class StorageSystemTableSettings final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemTableSettings"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8>) const override;
    Block getFilterSampleBlock() const override;
};

}
