#include "StorageSystemRemoteDataPaths.h"
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/IDisk.h>
#include <Disks/ObjectStorages/IMetadataStorage.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace fs = std::filesystem;

namespace DB
{
namespace Setting
{
    extern const SettingsBool traverse_shadow_remote_data_paths;
}

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int DIRECTORY_DOESNT_EXIST;
}


class SystemRemoteDataPathsSource : public ISource
{
public:
    SystemRemoteDataPathsSource(
        const DisksMap & disks_,
        Block header_,
        UInt64 max_block_size_,
        ContextPtr context_)
        : ISource(header_)
        , max_block_size(max_block_size_)
        , context(std::move(context_))
    {
        for (const auto & disk : disks_)
        {
            if (disk.second->isRemote())
                disks.push_back(disk);
        }

        /// Position at the first disk
        nextDisk();
    }

    String getName() const override { return "SystemRemoteDataPaths"; }

protected:
    Chunk generate() override;

private:
    /// Moves to the next file or disk in DFS order, if no more files or disks returns false
    bool nextFile();
    /// Moves to the next disk in the list, if no more disks returns false
    bool nextDisk();

    /// Check if the path is a table path like "store/364/3643ff83-0996-4a4a-a90b-a96e66a10c74"
    static bool isTablePath(const fs::path & path);

    /// Returns full local path of the current file
    fs::path getCurrentPath() const
    {
        fs::path path;
        for (const auto & dir : paths_stack)
            path /= dir.names[dir.position].name;
        return path;
    }

    /// Returns the skip predicate for the current path
    const auto & getCurrentSkipPredicate() const
    {
        chassert(!paths_stack.empty());
        chassert(paths_stack.back().position < static_cast<ssize_t>(paths_stack.back().names.size()));
        return paths_stack.back().names[paths_stack.back().position].skip_predicate;
    }

    static bool skipPredicateForShadowDir(const String & local_path)
    {
        // `shadow/{backup_name}/revision.txt` is not an object metadata file
        const auto path = fs::path(local_path);
        return path.filename() == "revision.txt" &&
                path.parent_path().has_parent_path() &&
                path.parent_path().parent_path().filename() == "shadow";
    }

    const UInt64 max_block_size;
    std::vector<std::pair<std::string, DiskPtr>> disks;
    ContextPtr context;

    /// Directory entry with optional predicate to skip some files
    struct NameAndFilter
    {
        std::string name;
        std::function<bool(const String &)> skip_predicate; /// Skip files that match the predicate in the subtree
    };

    /// Directory contents
    struct DirListingAndPosition
    {
        std::vector<NameAndFilter> names;
        ssize_t position = -1;  /// Index of the name we a currently pointing at, -1 means not started yet
    };

    ssize_t current_disk = -1;  /// Start from -1 to move to the first disk on the first call to nextDisk()
    std::vector<DirListingAndPosition> paths_stack; /// Represents the current path for DFS order traversal
};

class ReadFromSystemRemoteDataPaths final : public SourceStepWithFilter
{
public:
    ReadFromSystemRemoteDataPaths(
        DisksMap && disks_,
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        const Block & header,
        UInt64 max_block_size_)
        : SourceStepWithFilter(
            {.header = header},
            column_names_,
            query_info_,
            storage_snapshot_,
            context_)
        , storage_limits(query_info.storage_limits)
        , max_block_size(max_block_size_)
        , disks(std::move(disks_))
    {
    }

    String getName() const override { return "ReadFromSystemRemoteDataPaths"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    /// TODO: void applyFilters(ActionDAGNodes added_filter_nodes) can be implemented to filter out disk names

private:
    std::shared_ptr<const StorageLimitsList> storage_limits;
    const UInt64 max_block_size;
    DisksMap disks;
};


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

void StorageSystemRemoteDataPaths::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t max_block_size,
    const size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);
    auto header = storage_snapshot->metadata->getSampleBlockWithVirtuals(getVirtualsList());
    auto read_step = std::make_unique<ReadFromSystemRemoteDataPaths>(
        context->getDisksMap(),
        column_names,
        query_info,
        storage_snapshot,
        context,
        header,
        max_block_size);
    query_plan.addStep(std::move(read_step));
}

void ReadFromSystemRemoteDataPaths::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & /*settings*/)
{
    const auto & header = getOutputStream().header;
    auto source = std::make_shared<SystemRemoteDataPathsSource>(std::move(disks), header, max_block_size, context);
    source->setStorageLimits(storage_limits);
    processors.emplace_back(source);
    pipeline.init(Pipe(std::move(source)));
}

bool SystemRemoteDataPathsSource::nextDisk()
{
    while (current_disk < static_cast<ssize_t>(disks.size()))
    {
        paths_stack.clear();
        ++current_disk;

        if (current_disk >= static_cast<ssize_t>(disks.size()))
            break;

        auto & current = paths_stack.emplace_back();

        /// Add dirs that we want to traverse. It's ok if some of them don't exist because traversal logic handles
        /// cases when children of a directory get deleted while traversal is running.
        current.names.push_back({"store", nullptr});
        current.names.push_back({"data", nullptr});
        if (context->getSettingsRef()[Setting::traverse_shadow_remote_data_paths])
            current.names.push_back({"shadow", skipPredicateForShadowDir});

        /// Start and move to the first file
        current.position = -1;
        if (nextFile())
            return true;
    }
    return false;
}

/// Check if the path is a table path like "store/364/3643ff83-0996-4a4a-a90b-a96e66a10c74"
bool SystemRemoteDataPathsSource::isTablePath(const fs::path & path)
{
    std::vector<std::string> components;
    for (auto it = path.begin(); it != path.end(); ++it)
        components.push_back(it->string());

    return components.size() == 3
        && components[0] == "store"
        && components[1].size() == 3      /// "364"
        && components[2].size() == 36;    /// "3643ff83-0996-4a4a-a90b-a96e66a10c74"
}

bool SystemRemoteDataPathsSource::nextFile()
{
    while (true)
    {
        while (!paths_stack.empty())
        {
            auto & current = paths_stack.back();
            ++current.position;
            /// Move to the next child in the current directory
            if (current.position < static_cast<ssize_t>(current.names.size()))
                break;
            /// Move up to the parent directory if this was the last child
            paths_stack.pop_back();
        }

        /// Done with the current disk?
        if (paths_stack.empty())
            return false;

        const auto current_path = getCurrentPath();

        try
        {
            const auto & disk = disks[current_disk].second;

            /// Files or directories can disappear due to concurrent operations
            if (!disk->exists(current_path))
                continue;

            /// Stop if current path is a file
            if (disk->isFile(current_path))
                return true;

            /// If current path is a directory list its contents and step into it
            std::vector<std::string> children;
            disk->listFiles(current_path, children);

            /// Use current predicate for all children
            const auto & skip_predicate = getCurrentSkipPredicate();
            DirListingAndPosition dir;
            for (const auto & child : children)
                dir.names.push_back({child, skip_predicate});
            dir.position = -1;

            paths_stack.emplace_back(std::move(dir));
        }
        catch (const Exception & e)
        {
            /// Files or directories can disappear due to concurrent operations
            if (e.code() == ErrorCodes::FILE_DOESNT_EXIST ||
                e.code() == ErrorCodes::DIRECTORY_DOESNT_EXIST)
                continue;

            throw;
        }
        catch (const fs::filesystem_error & e)
        {
            /// Files or directories can disappear due to concurrent operations
            if (e.code() == std::errc::no_such_file_or_directory)
                continue;

            /// Skip path if it's table path and we don't have permissions to read it
            /// This can happen if the table is being dropped by first chmoding the directory to 000
            if (e.code() == std::errc::permission_denied && isTablePath(current_path))
                continue;

            throw;
        }
    }
}

Chunk SystemRemoteDataPathsSource::generate()
{
    /// Finish if all disks are processed
    if (current_disk >= static_cast<ssize_t>(disks.size()))
        return {};

    MutableColumnPtr col_disk_name = ColumnString::create();
    MutableColumnPtr col_base_path = ColumnString::create();
    MutableColumnPtr col_cache_base_path = ColumnString::create();
    MutableColumnPtr col_local_path = ColumnString::create();
    MutableColumnPtr col_remote_path = ColumnString::create();
    MutableColumnPtr col_size = ColumnUInt64::create();
    MutableColumnPtr col_namespace = ColumnString::create();
    MutableColumnPtr col_cache_paths = ColumnArray::create(ColumnString::create());

    QueryStatusPtr query_status = context->getProcessListElement();

    size_t row_count = 0;
    do
    {
        if (query_status)
            query_status->checkTimeLimit();

        /// Check if the block is big enough already
        if (max_block_size > 0 && row_count > 0)
        {
            size_t total_size =
                col_disk_name->byteSize() +
                col_base_path->byteSize() +
                col_cache_base_path->byteSize() +
                col_local_path->byteSize() +
                col_remote_path->byteSize() +
                col_size->byteSize() +
                col_namespace->byteSize() +
                col_cache_paths->byteSize();
            if (total_size > max_block_size)
                break;
        }

        const auto & [disk_name, disk] = disks[current_disk];
        auto local_path = getCurrentPath();

        const auto & skip_predicate = getCurrentSkipPredicate();
        if (skip_predicate && skip_predicate(local_path))
            continue;

        FileCachePtr cache;

        if (disk->supportsCache())
            cache = FileCacheFactory::instance().getByName(disk->getCacheName())->cache;

        StoredObjects storage_objects;
        try
        {
            storage_objects = disk->getMetadataStorage()->getStorageObjects(local_path);
        }
        catch (const Exception & e)
        {
            /// Unfortunately in rare cases it can happen when files disappear
            /// or can be empty in case of operation interruption (like cancelled metadata fetch)
            if (e.code() == ErrorCodes::FILE_DOESNT_EXIST ||
                e.code() == ErrorCodes::DIRECTORY_DOESNT_EXIST ||
                e.code() == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF ||
                e.code() == ErrorCodes::CANNOT_READ_ALL_DATA)
                continue;

            throw;
        }

        for (const auto & object : storage_objects)
        {
            ++row_count;
            col_disk_name->insert(disk_name);
            col_base_path->insert(disk->getPath());
            if (cache)
                col_cache_base_path->insert(cache->getBasePath());
            else
                col_cache_base_path->insertDefault();
            col_local_path->insert(local_path.string());
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
    while (nextFile() || nextDisk());

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

    return chunk;
}

}
