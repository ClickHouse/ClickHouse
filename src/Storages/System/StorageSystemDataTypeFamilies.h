#pragma once

#include <base/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class StorageSystemDataTypeFamilies final : public shared_ptr_helper<StorageSystemDataTypeFamilies>,
                                      public IStorageSystemOneBlock<StorageSystemDataTypeFamilies>
{
    friend struct shared_ptr_helper<StorageSystemDataTypeFamilies>;
protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;

public:
    std::string getName() const override { return "SystemTableDataTypeFamilies"; }

    static NamesAndTypesList getNamesAndTypes();
};

}
