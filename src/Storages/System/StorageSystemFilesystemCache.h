#pragma once

#include <base/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

/**
 * Usgae example. How to get mapping from local paths to remote paths:
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

class StorageSystemFilesystemCache final : public shared_ptr_helper<StorageSystemFilesystemCache>,
    public IStorageSystemOneBlock<StorageSystemFilesystemCache>
{
    friend struct shared_ptr_helper<StorageSystemFilesystemCache>;
public:
    std::string getName() const override { return "SystemFilesystemCache"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    explicit StorageSystemFilesystemCache(const StorageID & table_id_);

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
