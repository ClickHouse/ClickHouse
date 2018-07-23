#pragma once
#include <Storages/System/IStorageSystemWithStringColumns.h>
#include <ext/shared_ptr_helper.h>

namespace DB
{
class StorageSystemCollations : public ext::shared_ptr_helper<StorageSystemCollations>,
                                public IStorageSystemWithStringColumns<StorageSystemCollations>
{
protected:
    void fillData(MutableColumns & res_columns) const override;

public:
    using IStorageSystemWithStringColumns::IStorageSystemWithStringColumns;

    std::string getName() const override
    {
        return "SystemTableCollations";
    }

    static std::vector<String> getColumnNames()
    {
        return {"name"};
    }
};
}
