#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include <ext/shared_ptr_helper.h>


namespace DB
{
class Context;


/** System table "contributors" with list of clickhouse contributors
  */
class StorageSystemContributors final : public ext::shared_ptr_helper<StorageSystemContributors>,
                                  public IStorageSystemOneBlock<StorageSystemContributors>
{
    friend struct ext::shared_ptr_helper<StorageSystemContributors>;
protected:
    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo & query_info) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override
    {
        return "SystemContributors";
    }

    static NamesAndTypesList getNamesAndTypes();
};
}
