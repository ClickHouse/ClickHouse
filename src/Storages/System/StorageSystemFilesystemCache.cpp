#include "StorageSystemFilesystemCache.h"
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Common/IFileCache.h>
#include <Common/FileSegment.h>
#include <Common/FileCacheFactory.h>
#include <Interpreters/Context.h>
#include <Disks/IDisk.h>


namespace DB
{

NamesAndTypesList StorageSystemFilesystemCache::getNamesAndTypes()
{
    return {
        {"cache_base_path", std::make_shared<DataTypeString>()},
        {"cache_path", std::make_shared<DataTypeString>()},
        {"file_segment_range_begin", std::make_shared<DataTypeUInt64>()},
        {"file_segment_range_end", std::make_shared<DataTypeUInt64>()},
        {"size", std::make_shared<DataTypeUInt64>()},
        {"state", std::make_shared<DataTypeString>()},
        {"cache_hits", std::make_shared<DataTypeUInt64>()},
        {"references", std::make_shared<DataTypeUInt64>()},
        {"downloaded_size", std::make_shared<DataTypeUInt64>()},
    };
}

StorageSystemFilesystemCache::StorageSystemFilesystemCache(const StorageID & table_id_)
    : IStorageSystemOneBlock(table_id_)
{
}

void StorageSystemFilesystemCache::fillData(MutableColumns & res_columns, ContextPtr, const SelectQueryInfo &) const
{
    auto caches = FileCacheFactory::instance().getAll();

    for (const auto & [cache_base_path, cache_data] : caches)
    {
        const auto & cache = cache_data->cache;
        auto file_segments = cache->getSnapshot();

        for (const auto & file_segment : file_segments)
        {
            res_columns[0]->insert(cache_base_path);
            res_columns[1]->insert(
                cache->getPathInLocalCache(file_segment->key(), file_segment->offset(), file_segment->isPersistent()));

            const auto & range = file_segment->range();
            res_columns[2]->insert(range.left);
            res_columns[3]->insert(range.right);
            res_columns[4]->insert(range.size());
            res_columns[5]->insert(FileSegment::stateToString(file_segment->state()));
            res_columns[6]->insert(file_segment->getHitsCount());
            res_columns[7]->insert(file_segment->getRefCount());
            res_columns[8]->insert(file_segment->getDownloadedSize());
        }
    }
}

}
