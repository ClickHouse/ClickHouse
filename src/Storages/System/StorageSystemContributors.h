#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{
class Context;


/** System table "contributors" with list of clickhouse contributors
  */
class StorageSystemContributors final : public IStorageSystemOneBlock<StorageSystemContributors>
{
protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override
    {
        return "SystemContributors";
    }

    static NamesAndTypesList getNamesAndTypes();
};
}
