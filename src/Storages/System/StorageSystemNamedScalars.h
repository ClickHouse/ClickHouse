#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class StorageSystemNamedScalars final : public IStorageSystemOneBlock
{
public:
    StorageSystemNamedScalars(const StorageID & storage_id_, ColumnsDescription columns_description_);

    std::string getName() const override { return "SystemNamedScalars"; }

    static ColumnsDescription getColumnsDescription();

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
