#include <Storages/System/StorageSystemFilesystemCache.h>

#include <Columns/IColumn.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Disks/IDisk.h>


namespace DB
{

ColumnsDescription StorageSystemFilesystemCache::getColumnsDescription()
{
    /// TODO: Fill in all the comments.
    return ColumnsDescription
    {
        {"cache_name", std::make_shared<DataTypeString>(), "Name of the cache object. Can be used in `SYSTEM DESCRIBE FILESYSTEM CACHE <name>`, `SYSTEM DROP FILESYSTEM CACHE <name>` commands"},
        {"cache_base_path", std::make_shared<DataTypeString>(), "Path to the base directory where all caches files (of a cache identidied by `cache_name`) are stored."},
        {"cache_path", std::make_shared<DataTypeString>(), "Path to a particular cache file, corresponding to a file segment in a source file"},
        {"key", std::make_shared<DataTypeString>(), "Cache key of the file segment"},
        {"file_segment_range_begin", std::make_shared<DataTypeUInt64>(), "Offset corresponding to the beginning of the file segment range"},
        {"file_segment_range_end", std::make_shared<DataTypeUInt64>(), "Offset corresponding to the (including) end of the file segment range"},
        {"size", std::make_shared<DataTypeUInt64>(), "Size of the file segment"},
        {"state", std::make_shared<DataTypeString>(), "File segment state (DOWNLOADED, DOWNLOADING, PARTIALLY_DOWNLOADED, ...)"},
        {"finished_download_time", std::make_shared<DataTypeDateTime>(), "Time when file segment finished downloading."},
        {"cache_hits", std::make_shared<DataTypeUInt64>(), "Number of cache hits of corresponding file segment"},
        {"references", std::make_shared<DataTypeUInt64>(), "Number of references to corresponding file segment. Value 1 means that nobody uses it at the moment (the only existing reference is in cache storage itself)"},
        {"downloaded_size", std::make_shared<DataTypeUInt64>(), "Downloaded size of the file segment"},
        {"kind", std::make_shared<DataTypeString>(), "File segment kind (used to distringuish between file segments added as a part of 'Temporary data in cache')"},
        {"unbound", std::make_shared<DataTypeNumber<UInt8>>(), "Internal implementation flag"},
        {"user_id", std::make_shared<DataTypeString>(), "User id of the user which created the file segment"},
        {"file_size", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "File size of the file to which current file segment belongs"},
    };
}

StorageSystemFilesystemCache::StorageSystemFilesystemCache(const StorageID & table_id_)
    : IStorageSystemOneBlock(table_id_, getColumnsDescription())
{
}

void StorageSystemFilesystemCache::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto caches_by_name = FileCacheFactory::instance().getAll();
    std::unordered_set<FileCacheFactory::FileCacheDataPtr> caches;
    for (const auto & [_, cache_data] : caches_by_name)
        caches.insert(cache_data);
    std::unordered_map<FileCacheFactory::FileCacheDataPtr, std::vector<std::string>> caches_by_instance;
    for (const auto & [cache_name, cache_data] : caches_by_name)
        caches_by_instance[cache_data].push_back(cache_name);

    for (const auto & cache_data : caches)
    {
        const auto & cache = cache_data->cache;
        if (!cache->isInitialized())
            continue;

        cache->iterate([&](const FileSegment::Info & file_segment)
        {
            for (const auto & cache_name : caches_by_instance.at(cache_data))
            {
                size_t i = 0;
                res_columns[i++]->insert(cache_name);
                res_columns[i++]->insert(cache->getBasePath());

                /// Do not use `file_segment->getPath` here because it will lead to nullptr dereference
                /// (because file_segments in getSnapshot doesn't have `cache` field set)

                const auto path = cache->getFileSegmentPath(
                    file_segment.key, file_segment.offset, file_segment.kind,
                    FileCache::UserInfo(file_segment.user_id, file_segment.user_weight));
                res_columns[i++]->insert(path);
                res_columns[i++]->insert(file_segment.key.toString());
                res_columns[i++]->insert(file_segment.range_left);
                res_columns[i++]->insert(file_segment.range_right);
                res_columns[i++]->insert(file_segment.size);
                res_columns[i++]->insert(FileSegment::stateToString(file_segment.state));
                res_columns[i++]->insert(file_segment.download_finished_time);
                res_columns[i++]->insert(file_segment.cache_hits);
                res_columns[i++]->insert(file_segment.references);
                res_columns[i++]->insert(file_segment.downloaded_size);
                res_columns[i++]->insert(toString(file_segment.kind));
                res_columns[i++]->insert(file_segment.is_unbound);
                res_columns[i++]->insert(file_segment.user_id);

                std::error_code ec;
                auto size = fs::file_size(path, ec);
                if (!ec)
                    res_columns[i++]->insert(size);
                else
                    res_columns[i++]->insertDefault();
            }
        }, FileCache::getCommonUser().user_id);
    }
}

}
