#pragma once

#include <Storages/System/IStorageSystemWithStringColumns.h>
#include <ext/shared_ptr_helper.h>
namespace DB
{
class StorageSystemTableFunctions : public ext::shared_ptr_helper<StorageSystemTableFunctions>,
                                    public IStorageSystemWithStringColumns<StorageSystemTableFunctions>
{
protected:
    void fillData(MutableColumns & res_columns) const override;

public:
    using IStorageSystemWithStringColumns::IStorageSystemWithStringColumns;

    std::string getName() const override
    {
        return "SystemTableFunctions";
    }

    static std::vector<String> getColumnNames()
    {
        return {"name"};
    }
};
}
