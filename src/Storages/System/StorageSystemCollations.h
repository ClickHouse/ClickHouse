#pragma once
#include <Storages/System/IStorageSystemOneBlock.h>
#include <common/shared_ptr_helper.h>

namespace DB
{

class StorageSystemCollations final : public shared_ptr_helper<StorageSystemCollations>,
                                public IStorageSystemOneBlock<StorageSystemCollations>
{
    friend struct shared_ptr_helper<StorageSystemCollations>;
protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;
public:

    std::string getName() const override { return "SystemTableCollations"; }

    static NamesAndTypesList getNamesAndTypes();
};

}
