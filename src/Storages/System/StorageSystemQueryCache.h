#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class StorageSystemQueryCache final : public IStorageSystemOneBlock<StorageSystemQueryCache>
{
public:
    explicit StorageSystemQueryCache(const StorageID & table_id_);

    std::string getName() const override { return "SystemQueryCache"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
