#include "StorageSystemFilesystemCache.h"
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Interpreters/Context.h>
#include <Disks/IDisk.h>


namespace DB
{

ColumnsDescription StorageSystemFilesystemCache::getColumnsDescription()
{
    /// TODO: Fill in all the comments.
    return ColumnsDescription
    {
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
        cache->iterate([&](const FileSegment::Info & file_segment)
        {
            size_t i = 0;
            res_columns[i++]->insert(cache_name);
            res_columns[i++]->insert(cache->getBasePath());

            /// Do not use `file_segment->getPathInLocalCache` here because it will lead to nullptr dereference
            /// (because file_segments in getSnapshot doesn't have `cache` field set)

            const auto path = cache->getPathInLocalCache(file_segment.key, file_segment.offset, file_segment.kind);
            res_columns[i++]->insert(path);
            res_columns[i++]->insert(file_segment.key.toString());
            res_columns[i++]->insert(file_segment.range_left);
            res_columns[i++]->insert(file_segment.range_right);
            res_columns[i++]->insert(file_segment.size);
            res_columns[i++]->insert(FileSegment::stateToString(file_segment.state));
            res_columns[i++]->insert(file_segment.cache_hits);
            res_columns[i++]->insert(file_segment.references);
            res_columns[i++]->insert(file_segment.downloaded_size);
            res_columns[i++]->insert(toString(file_segment.kind));
            res_columns[i++]->insert(file_segment.is_unbound);

            std::error_code ec;
            auto size = fs::file_size(path, ec);
            if (!ec)
                res_columns[i++]->insert(size);
            else
                res_columns[i++]->insertDefault();
        });
    }
}

}
