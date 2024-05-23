#include "StorageSystemRemoteDataPaths.h"
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Interpreters/Context.h>
#include <Disks/IDisk.h>

namespace fs = std::filesystem;

namespace DB
{

StorageSystemRemoteDataPaths::StorageSystemRemoteDataPaths(const StorageID & table_id_)
    : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription(
    {
        {"disk_name", std::make_shared<DataTypeString>(), "Disk name."},
        {"path", std::make_shared<DataTypeString>(), "Disk path."},
        {"cache_base_path", std::make_shared<DataTypeString>(), "Base directory of cache files."},
        {"local_path", std::make_shared<DataTypeString>(), "Path of ClickHouse file, also used as metadata path."},
        {"remote_path", std::make_shared<DataTypeString>(), "Blob path in object storage, with which ClickHouse file is associated with."},
        {"size", std::make_shared<DataTypeUInt64>(), "Size of the file (compressed)."},
        {"common_prefix_for_blobs", std::make_shared<DataTypeString>(), "Common prefix for blobs in object storage."},
        {"cache_paths", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Cache files for corresponding blob."},
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
    const size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

    MutableColumnPtr col_disk_name = ColumnString::create();
    MutableColumnPtr col_base_path = ColumnString::create();
    MutableColumnPtr col_cache_base_path = ColumnString::create();
    MutableColumnPtr col_local_path = ColumnString::create();
    MutableColumnPtr col_remote_path = ColumnString::create();
    MutableColumnPtr col_size = ColumnUInt64::create();
    MutableColumnPtr col_namespace = ColumnString::create();
    MutableColumnPtr col_cache_paths = ColumnArray::create(ColumnString::create());

    auto disks = context->getDisksMap();
    for (const auto & [disk_name, disk] : disks)
    {
        if (disk->isRemote())
        {
            std::vector<IDisk::LocalPathWithObjectStoragePaths> remote_paths_by_local_path;
            disk->getRemotePathsRecursive("store", remote_paths_by_local_path, /* skip_predicate = */ {});
            disk->getRemotePathsRecursive("data", remote_paths_by_local_path, /* skip_predicate = */ {});
            if (context->getSettingsRef().traverse_shadow_remote_data_paths)
                disk->getRemotePathsRecursive(
                    "shadow",
                    remote_paths_by_local_path,
                    [](const String & local_path)
                    {
                        // `shadow/{backup_name}/revision.txt` is not an object metadata file
                        const auto path = fs::path(local_path);
                        return path.filename() == "revision.txt" &&
                               path.parent_path().has_parent_path() &&
                               path.parent_path().parent_path().filename() == "shadow";
                    });

            FileCachePtr cache;

            if (disk->supportsCache())
                cache = FileCacheFactory::instance().getByName(disk->getCacheName())->cache;

            for (const auto & [local_path, storage_objects] : remote_paths_by_local_path)
            {
                for (const auto & object : storage_objects)
                {
                    col_disk_name->insert(disk_name);
                    col_base_path->insert(disk->getPath());
                    if (cache)
                        col_cache_base_path->insert(cache->getBasePath());
                    else
                        col_cache_base_path->insertDefault();
                    col_local_path->insert(local_path);
                    col_remote_path->insert(object.remote_path);
                    col_size->insert(object.bytes_size);

                    col_namespace->insertDefault();

                    if (cache)
                    {
                        auto cache_paths = cache->tryGetCachePaths(cache->createKeyForPath(object.remote_path));
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
    res_columns.emplace_back(std::move(col_size));
    res_columns.emplace_back(std::move(col_namespace));
    res_columns.emplace_back(std::move(col_cache_paths));

    UInt64 num_rows = res_columns.at(0)->size();
    Chunk chunk(std::move(res_columns), num_rows);

    return Pipe(std::make_shared<SourceFromSingleChunk>(storage_snapshot->metadata->getSampleBlock(), std::move(chunk)));
}

}
