#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include <Storages/Cache/SchemaCache.h>

namespace DB
{

class StorageSystemSchemaInferenceCache final : public IStorageSystemOneBlock<StorageSystemSchemaInferenceCache>
{
public:
    std::string getName() const override { return "SystemSettingsChanges"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;

    void fillDataImpl(MutableColumns & res_columns, SchemaCache & schema_cache, const String & storage_name) const;
};

}
