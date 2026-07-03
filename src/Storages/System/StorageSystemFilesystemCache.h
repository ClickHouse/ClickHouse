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

class StorageSystemFilesystemCache final : public IStorageSystemOneBlock
{
public:
    explicit StorageSystemFilesystemCache(const StorageID & table_id_);

    std::string getName() const override { return "SystemFilesystemCache"; }

    static ColumnsDescription getColumnsDescription();

protected:
    void fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
