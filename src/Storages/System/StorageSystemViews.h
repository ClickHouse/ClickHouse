#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class StorageSystemViews final : public ext::shared_ptr_helper<StorageSystemViews>, public IStorageSystemOneBlock<StorageSystemViews>
{
    friend struct ext::shared_ptr_helper<StorageSystemViews>;
protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo & query_info) const override;

public:
    std::string getName() const override { return "SystemViews"; }

    static NamesAndTypesList getNamesAndTypes();

};

}
