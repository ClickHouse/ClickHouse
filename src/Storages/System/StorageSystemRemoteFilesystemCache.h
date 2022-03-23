#pragma once

#include <base/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

/**
 * SELECT
 *     cache_path,
 *     cache_hits,
 *     remote_path,
 *     local_path,
 *     file_segment_range,
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
 * INNER JOIN system.remote_filesystem_cache AS caches ON data_paths.cache_path = caches.cache_path
 * FORMAT Vertical
 */

class StorageSystemRemoteFilesystemCache final : public shared_ptr_helper<StorageSystemRemoteFilesystemCache>,
    public IStorageSystemOneBlock<StorageSystemRemoteFilesystemCache>
{
    friend struct shared_ptr_helper<StorageSystemRemoteFilesystemCache>;
public:
    std::string getName() const override { return "SystemRemoteFilesystemCache"; }

    static NamesAndTypesList getNamesAndTypes();

protected:
    explicit StorageSystemRemoteFilesystemCache(const StorageID & table_id_);

    void fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo & query_info) const override;
};

}
