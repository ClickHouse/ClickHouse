#pragma once

#include <Storages/Cache/SchemaCache.h>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class StorageSystemSchemaInferenceCache final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemSettingsChanges"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
