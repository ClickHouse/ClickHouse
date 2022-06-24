#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class StorageSystemAllocations final : public IStorageSystemOneBlock<StorageSystemAllocations>
{
public:
    explicit StorageSystemAllocations(const StorageID & table_id_);

    String getName() const override { return "SystemAllocations"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
