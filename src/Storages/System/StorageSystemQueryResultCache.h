#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class StorageSystemQueryResultCache final : public IStorageSystemOneBlock<StorageSystemQueryResultCache>
{
public:
    explicit StorageSystemQueryResultCache(const StorageID & table_id_);

    std::string getName() const override { return "SystemQueryResultCache"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
