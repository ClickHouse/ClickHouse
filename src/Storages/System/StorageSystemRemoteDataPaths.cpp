#include "StorageSystemRemoteDataPaths.h"
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <Common/FileCache.h>
#include <Common/FileCacheFactory.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Interpreters/Context.h>
#include <Disks/IDisk.h>


namespace DB
{

StorageSystemRemoteDataPaths::StorageSystemRemoteDataPaths(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription(
    {
        {"disk_name", std::make_shared<DataTypeString>()},
        {"path", std::make_shared<DataTypeString>()},
        {"cache_base_path", std::make_shared<DataTypeString>()},
        {"local_path", std::make_shared<DataTypeString>()},
        {"remote_path", std::make_shared<DataTypeString>()},
        {"cache_paths", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
    }));
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageSystemRemoteDataPaths::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    storage_snapshot->check(column_names);

    MutableColumnPtr col_disk_name = ColumnString::create();
    MutableColumnPtr col_base_path = ColumnString::create();
    MutableColumnPtr col_cache_base_path = ColumnString::create();
    MutableColumnPtr col_local_path = ColumnString::create();
    MutableColumnPtr col_remote_path = ColumnString::create();
    MutableColumnPtr col_cache_paths = ColumnArray::create(ColumnString::create());

    auto disks = context->getDisksMap();
    for (const auto & [disk_name, disk] : disks)
    {
        if (disk->isRemote())
        {
            std::vector<IDisk::LocalPathWithObjectStoragePaths> remote_paths_by_local_path;
            disk->getRemotePathsRecursive("store", remote_paths_by_local_path);
            disk->getRemotePathsRecursive("data", remote_paths_by_local_path);

            FileCachePtr cache;
            auto cache_base_path = disk->supportsCache() ? disk->getCacheBasePath() : "";

            if (!cache_base_path.empty())
                cache = FileCacheFactory::instance().get(cache_base_path);

            for (const auto & [local_path, storage_objects] : remote_paths_by_local_path)
            {
                for (const auto & object : storage_objects)
                {
                    col_disk_name->insert(disk_name);
                    col_base_path->insert(disk->getPath());
                    col_cache_base_path->insert(cache_base_path);
                    col_local_path->insert(local_path);
                    col_remote_path->insert(object.absolute_path);

                    if (cache)
                    {
                        auto cache_paths = cache->tryGetCachePaths(cache->hash(object.getPathKeyForCache()));
                        col_cache_paths->insert(Array(cache_paths.begin(), cache_paths.end()));
                    }
                    else
                    {
                        col_cache_paths->insertDefault();
                    }
                }
            }
        }
    }

    Columns res_columns;
    res_columns.emplace_back(std::move(col_disk_name));
    res_columns.emplace_back(std::move(col_base_path));
    res_columns.emplace_back(std::move(col_cache_base_path));
    res_columns.emplace_back(std::move(col_local_path));
    res_columns.emplace_back(std::move(col_remote_path));
    res_columns.emplace_back(std::move(col_cache_paths));

    UInt64 num_rows = res_columns.at(0)->size();
    Chunk chunk(std::move(res_columns), num_rows);

    return Pipe(std::make_shared<SourceFromSingleChunk>(storage_snapshot->metadata->getSampleBlock(), std::move(chunk)));
}

}
