#pragma once

#include <optional>
#include <Parsers/ASTIdentifier.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

/** Implements a table engine for the system "iceberg_history".
 *
 * db_name String
 * table_name String
 * made_current_at DateTime64,
 * snapshot_id UInt64,
 * parent_id UInt64,
 * is_current_ancestor Bool
 *
 */

class StorageSystemIcebergHistory final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemIcebergHistory"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData([[maybe_unused]] MutableColumns & res_columns, [[maybe_unused]] ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
