#pragma once

#include <optional>
#include <Parsers/ASTIdentifier.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

/** Implements a system table for Delta Lake tables history.
 * It provides information about the various versions of Delta Lake tables created in ClickHouse.
 *
 * database String
 * table String
 * version UInt64
 * timestamp DateTime64(3)
 * operation String
 * operation_parameters Map(String, String)
 * is_current UInt8
 *
 */

class StorageSystemDeltaLakeHistory final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemDeltaLakeHistory"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(
        [[maybe_unused]] MutableColumns & res_columns,
        [[maybe_unused]] ContextPtr context,
        const ActionsDAG::Node *,
        std::vector<UInt8>) const override;
};

}
