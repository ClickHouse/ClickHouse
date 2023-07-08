#include "StorageSystemFilesystemCache.h"
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Interpreters/Context.h>
#include <Disks/IDisk.h>


namespace DB
{

NamesAndTypesList StorageSystemFilesystemCache::getNamesAndTypes()
{
    return {
        {"cache_name", std::make_shared<DataTypeString>()},
        {"cache_base_path", std::make_shared<DataTypeString>()},
        {"cache_path", std::make_shared<DataTypeString>()},
        {"file_segment_range_begin", std::make_shared<DataTypeUInt64>()},
        {"file_segment_range_end", std::make_shared<DataTypeUInt64>()},
        {"size", std::make_shared<DataTypeUInt64>()},
        {"state", std::make_shared<DataTypeString>()},
        {"cache_hits", std::make_shared<DataTypeUInt64>()},
        {"references", std::make_shared<DataTypeUInt64>()},
        {"downloaded_size", std::make_shared<DataTypeUInt64>()},
        {"persistent", std::make_shared<DataTypeNumber<UInt8>>()},
        {"kind", std::make_shared<DataTypeString>()},
        {"unbound", std::make_shared<DataTypeNumber<UInt8>>()},
    };
}

StorageSystemFilesystemCache::StorageSystemFilesystemCache(const StorageID & table_id_)
    : IStorageSystemOneBlock(table_id_)
{
}

void StorageSystemFilesystemCache::fillData(MutableColumns & res_columns, ContextPtr, const SelectQueryInfo &) const
{
    auto caches = FileCacheFactory::instance().getAll();

    for (const auto & [cache_name, cache_data] : caches)
    {
        const auto & cache = cache_data->cache;
        auto file_segments = cache->getSnapshot();

        for (const auto & file_segment : file_segments)
        {
            res_columns[0]->insert(cache_name);
            res_columns[1]->insert(cache->getBasePath());

            /// Do not use `file_segment->getPathInLocalCache` here because it will lead to nullptr dereference
            /// (because file_segments in getSnapshot doesn't have `cache` field set)
            res_columns[2]->insert(
                cache->getPathInLocalCache(file_segment->key(), file_segment->offset(), file_segment->getKind()));

            const auto & range = file_segment->range();
            res_columns[3]->insert(range.left);
            res_columns[4]->insert(range.right);
            res_columns[5]->insert(range.size());
            res_columns[6]->insert(FileSegment::stateToString(file_segment->state()));
            res_columns[7]->insert(file_segment->getHitsCount());
            res_columns[8]->insert(file_segment->getRefCount());
            res_columns[9]->insert(file_segment->getDownloadedSize());
            res_columns[10]->insert(file_segment->isPersistent());
            res_columns[11]->insert(toString(file_segment->getKind()));
            res_columns[12]->insert(file_segment->isUnbound());
        }
    }
}

}
