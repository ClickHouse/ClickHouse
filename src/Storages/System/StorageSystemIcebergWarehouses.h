#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

/// Warehouses of the Iceberg REST catalog as the `system.iceberg_warehouses` table.
class StorageSystemIcebergWarehouses final : public IStorageSystemOneBlock
{
public:
    explicit StorageSystemIcebergWarehouses(const StorageID & table_id_);

    std::string getName() const override { return "SystemIcebergWarehouses"; }

    static ColumnsDescription getColumnsDescription();

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
