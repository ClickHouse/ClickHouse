#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>
#include <Interpreters/Cache/FileCache_fwd_internal.h>

namespace DB
{

/**
 * Usage example. How to get mapping from local paths to remote paths:
 * SELECT
 *     cache_path,
 *     cache_hits,
 *     remote_path,
 *     local_path,
 *     file_segment_range_begin,
 *     file_segment_range_end,
 *     size,
 *     state
 * FROM
 * (
 *     SELECT
 *         arrayJoin(cache_paths) AS cache_path,
 *         local_path,
 *         remote_path
 *     FROM system.remote_data_paths
 * ) AS data_paths
 * INNER JOIN system.filesystem_cache AS caches ON data_paths.cache_path = caches.cache_path
 * FORMAT Vertical
 */

class StorageSystemFilesystemCache final : public IStorage
{
public:
    explicit StorageSystemFilesystemCache(const StorageID & table_id_);

    std::string getName() const override { return "SystemFilesystemCache"; }

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
