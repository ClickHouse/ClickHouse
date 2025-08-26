#pragma once

#include <Parsers/ASTIdentifier.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

/** Implements a table engine for Iceberg tables. It gives information about the various snapshots of Iceberg tables created in ClickHouse.
 *
event_time DateTime
path String
file_name String
content String
 *
 */

class StorageSystemIcebergMetadata final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "StorageSystemIcebergMetadata"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData([[maybe_unused]] MutableColumns & res_columns, [[maybe_unused]] ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
