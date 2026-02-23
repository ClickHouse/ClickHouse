#pragma once

#include <Storages/IStorage.h>


namespace DB
{

class Context;

/** Implements a system table for Delta Lake tables history.
 * Uses streaming mode to produce results in chunks, allowing LIMIT to work
 * and avoiding the need for an arbitrary cap on the number of records.
 *
 * database String
 * table String
 * version UInt64
 * timestamp Nullable(DateTime64(3))
 * operation String
 * operation_parameters Map(String, String)
 * is_latest_version UInt8
 *
 */

class StorageSystemDeltaLakeHistory final : public IStorage
{
public:
    explicit StorageSystemDeltaLakeHistory(const StorageID & table_id_);

    std::string getName() const override { return "SystemDeltaLakeHistory"; }
    bool isSystemStorage() const override { return true; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;
};

}
