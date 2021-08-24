#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class StorageSystemTableViews final : public ext::shared_ptr_helper<StorageSystemTableViews>, public IStorageSystemOneBlock<StorageSystemTableViews>
{
    friend struct ext::shared_ptr_helper<StorageSystemTableViews>;
protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo & query_info) const override;

public:
    std::string getName() const override { return "TableViews"; }

    static NamesAndTypesList getNamesAndTypes();

};

}
