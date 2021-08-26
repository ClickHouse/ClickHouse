#pragma once

#include <common/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class StorageSystemViews final : public shared_ptr_helper<StorageSystemViews>, public IStorageSystemOneBlock<StorageSystemViews>
{
    friend struct shared_ptr_helper<StorageSystemViews>;
protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const override;

public:
    std::string getName() const override { return "SystemViews"; }

    static NamesAndTypesList getNamesAndTypes();

};

}
