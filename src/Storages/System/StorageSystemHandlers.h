#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

/// Exposes SQL-defined HTTP handlers (CREATE HANDLER) as the `system.handlers` table.
class StorageSystemHandlers final : public IStorageSystemOneBlock
{
public:
    explicit StorageSystemHandlers(const StorageID & table_id_);

    std::string getName() const override { return "SystemHandlers"; }

    static ColumnsDescription getColumnsDescription();

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
