#pragma once

#include <Storages/Cache/SchemaCache.h>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class StorageSystemSchemaInferenceCache final : public IStorageSystemOneBlock<StorageSystemSchemaInferenceCache>
{
public:
    std::string getName() const override { return "SystemSettingsChanges"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
