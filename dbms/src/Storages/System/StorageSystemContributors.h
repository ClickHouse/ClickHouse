#pragma once

#if __has_include("StorageSystemContributors.generated.cpp")

#include <Storages/System/IStorageSystemOneBlock.h>
#include <ext/shared_ptr_helper.h>


namespace DB
{
class Context;


/** System table "contributors" with list of clickhouse contributors
  */
class StorageSystemContributors : public ext::shared_ptr_helper<StorageSystemContributors>,
                                  public IStorageSystemOneBlock<StorageSystemContributors>
{
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

#endif
