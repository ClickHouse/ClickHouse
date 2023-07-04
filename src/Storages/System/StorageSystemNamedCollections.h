#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class StorageSystemNamedCollections final : public IStorageSystemOneBlock<StorageSystemNamedCollections>
{
public:
    explicit StorageSystemNamedCollections(const StorageID & table_id_);

    std::string getName() const override { return "SystemNamedCollections"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
