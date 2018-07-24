#pragma once

#include <Storages/System/IStorageSystemWithStringColumns.h>
#include <ext/shared_ptr_helper.h>
namespace DB
{
class StorageSystemAggregateFunctionCombinators : public ext::shared_ptr_helper<StorageSystemAggregateFunctionCombinators>,
                                                  public IStorageSystemWithStringColumns<StorageSystemAggregateFunctionCombinators>
{
protected:
    void fillData(MutableColumns & res_columns) const override;

public:
    using IStorageSystemWithStringColumns::IStorageSystemWithStringColumns;

    std::string getName() const override
    {
        return "SystemAggregateFunctionCombinators";
    }

    static std::vector<String> getColumnNames()
    {
        return {"name"};
    }
};
}
