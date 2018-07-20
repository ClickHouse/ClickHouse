#pragma once

#include <Storages/System/IStorageSystemWithStringColumns.h>
#include <ext/shared_ptr_helper.h>

namespace DB
{
class StorageSystemFormats : public ext::shared_ptr_helper<StorageSystemFormats>, public IStorageSystemWithStringColumns<StorageSystemFormats>
{
protected:
    void fillData(MutableColumns & res_columns) const override;
public:
    using IStorageSystemWithStringColumns::IStorageSystemWithStringColumns;

    std::string getName() const override
    {
        return "SystemFormats";
    }

    static std::vector<String> getColumnNames()
    {
        return {"name", "type"};
    }

};
}
