#pragma once
#include <Storages/System/IStorageSystemWithStringColumns.h>
#include <ext/shared_ptr_helper.h>
namespace DB
{
class StorageSystemDataTypeFamilies : public ext::shared_ptr_helper<StorageSystemDataTypeFamilies>,
                                      public IStorageSystemWithStringColumns<StorageSystemDataTypeFamilies>
{
protected:
    void fillData(MutableColumns & res_columns) const override;

public:
    using IStorageSystemWithStringColumns::IStorageSystemWithStringColumns;

    std::string getName() const override
    {
        return "SystemTableDataTypeFamilies";
    }

    static std::vector<String> getColumnNames()
    {
        return {"name", "properties"};
    }
};
}
