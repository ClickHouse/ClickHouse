#pragma once

#include <DataTypes/DataTypeString.h>
#include <Storages/System/IStorageSystemOneBlock.h>
#include <common/shared_ptr_helper.h>

namespace DB
{

class StorageSystemTableEngines final : public shared_ptr_helper<StorageSystemTableEngines>,
                                  public IStorageSystemOneBlock<StorageSystemTableEngines>
{
    friend struct shared_ptr_helper<StorageSystemTableEngines>;
protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override
    {
        return "SystemTableEngines";
    }

    static NamesAndTypesList getNamesAndTypes();
};

}
