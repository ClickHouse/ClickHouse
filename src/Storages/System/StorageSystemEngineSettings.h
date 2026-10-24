#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

/// Implements `system.engine_settings`: the engine-specific settings of table engines, one row per engine and setting.
class StorageSystemEngineSettings final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemEngineSettings"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
