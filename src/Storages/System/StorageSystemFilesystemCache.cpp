#include "StorageSystemFilesystemCache.h"
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>
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
        {"key", std::make_shared<DataTypeString>()},
        {"file_segment_range_begin", std::make_shared<DataTypeUInt64>()},
        {"file_segment_range_end", std::make_shared<DataTypeUInt64>()},
        {"size", std::make_shared<DataTypeUInt64>()},
        {"state", std::make_shared<DataTypeString>()},
        {"cache_hits", std::make_shared<DataTypeUInt64>()},
        {"references", std::make_shared<DataTypeUInt64>()},
        {"downloaded_size", std::make_shared<DataTypeUInt64>()},
        {"kind", std::make_shared<DataTypeString>()},
        {"unbound", std::make_shared<DataTypeNumber<UInt8>>()},
        {"file_size", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>())},
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
        const auto file_segments = cache->getSnapshot();
        for (const auto & file_segment : file_segments)
        {
            size_t i = 0;
            res_columns[i++]->insert(cache_name);
            res_columns[i++]->insert(cache->getBasePath());

            /// Do not use `file_segment->getPathInLocalCache` here because it will lead to nullptr dereference
            /// (because file_segments in getSnapshot doesn't have `cache` field set)
            const auto path = cache->getPathInLocalCache(file_segment->key(), file_segment->offset(), file_segment->getKind());
            res_columns[i++]->insert(path);
            res_columns[i++]->insert(file_segment->key().toString());

            const auto & range = file_segment->range();
            res_columns[i++]->insert(range.left);
            res_columns[i++]->insert(range.right);
            res_columns[i++]->insert(range.size());
            res_columns[i++]->insert(FileSegment::stateToString(file_segment->state()));
            res_columns[i++]->insert(file_segment->getHitsCount());
            res_columns[i++]->insert(file_segment->getRefCount());
            res_columns[i++]->insert(file_segment->getDownloadedSize());
            res_columns[i++]->insert(toString(file_segment->getKind()));
            res_columns[i++]->insert(file_segment->isUnbound());
            try
            {
                if (fs::exists(path))
                    res_columns[i++]->insert(fs::file_size(path));
                else
                    res_columns[i++]->insertDefault();
            }
            catch (...)
            {
                res_columns[i++]->insertDefault();
            }
        }
    }
}

}
