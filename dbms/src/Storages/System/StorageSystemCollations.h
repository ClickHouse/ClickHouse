#pragma once
#include <Storages/System/IStorageSystemOneBlock.h>
#include <ext/shared_ptr_helper.h>

namespace DB
{

class StorageSystemCollations final : public StorageHelper<StorageSystemCollations>,
                                public IStorageSystemOneBlock<StorageSystemCollations>
{
    friend struct StorageHelper<StorageSystemCollations>;
protected:
    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo & query_info) const override;

    using IStorageSystemOneBlock::IStorageSystemOneBlock;
public:

    std::string getName() const override { return "SystemTableCollations"; }

    static NamesAndTypesList getNamesAndTypes();
};

}
