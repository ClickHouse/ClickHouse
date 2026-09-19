#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

/**
 * System table for introspecting columns cache.
 *
 * Shows all cached column blocks with:
 * - Table and part identification
 * - Column name
 * - Row range (row_begin, row_end)
 * - Cache entry size
 * - Number of rows
 *
 * Usage example:
 * SELECT
 *     table,
 *     part,
 *     column,
 *     row_begin,
 *     row_end,
 *     rows,
 *     bytes
 * FROM system.columns_cache
 * WHERE table = 'my_table'
 * ORDER BY part, column, row_begin
 */

class StorageSystemColumnsCache final : public IStorage
{
public:
    explicit StorageSystemColumnsCache(const StorageID & table_id_);

    std::string getName() const override { return "SystemColumnsCache"; }

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
