#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include "Interpreters/StorageID.h"


namespace DB
{

class Context;


class StorageSystemDictionaries final : public IStorageSystemOneBlock<StorageSystemDictionaries>
{
public:
    explicit StorageSystemDictionaries(const StorageID & storage_id_);

    std::string getName() const override { return "SystemDictionaries"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
