#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

/// Implements system table "engine_settings" which shows all settings for all table engines.
class StorageSystemEngineSettings final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemEngineSettings"; }

    static ColumnsDescription getColumnsDescription();

protected:
    bool supportsColumnsMask() const override { return true; }
    Block getFilterSampleBlock() const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
