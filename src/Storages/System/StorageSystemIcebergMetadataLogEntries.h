#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

/** Implements a table engine for Iceberg tables. It gives information about the metadata files of Iceberg tables created in ClickHouse.
 *
 * database String
 * table String
 * timestamp DateTime64(3)
 * file String
 * latest_snapshot_id Nullable(UInt64)
 * latest_schema_id Nullable(Int32)
 * latest_sequence_number Nullable(Int64)
 *
 */

class StorageSystemIcebergMetadataLogEntries final : public IStorageSystemOneBlock
{
public:
    std::string getName() const override { return "SystemIcebergMetadataLogEntries"; }

    static ColumnsDescription getColumnsDescription();

protected:
    using IStorageSystemOneBlock::IStorageSystemOneBlock;

    void fillData(
        [[maybe_unused]] MutableColumns & res_columns,
        [[maybe_unused]] ContextPtr context,
        [[maybe_unused]] const ActionsDAG::Node * predicate,
        std::vector<UInt8>) const override;

    Block getFilterSampleBlock() const override;
};

}
