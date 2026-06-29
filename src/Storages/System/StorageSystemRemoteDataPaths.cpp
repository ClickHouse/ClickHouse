#include <Storages/System/StorageSystemRemoteDataPaths.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/DiskType.h>
#include <Disks/IDisk.h>
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableLayout.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Interpreters/FileCache/FileCache.h>
#include <Interpreters/FileCache/FileCacheFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/VirtualColumnUtils.h>

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


class SystemRemoteDataPathsSource final : public ISource
{
public:
    SystemRemoteDataPathsSource(
        const DisksMap & disks_,
        SharedHeader header_,
        UInt64 max_block_size_,
        ContextPtr context_)
        : ISource(header_)
        , max_block_size(max_block_size_)
        , context(std::move(context_))
    {
        for (const auto & disk : disks_)
        {
            /// plain_rewritable disks are included even when not "remote" (e.g. local object storage used
            /// in tests), because their blob layout is enumerated from the in-memory metadata tree below.
            if (disk.second->isRemote()
                || disk.second->getDataSourceDescription().metadata_type == MetadataStorageType::PlainRewritable)
                disks.push_back(disk);
        }

        auto component_guard = Coordination::setCurrentComponent("SystemRemoteDataPathsSource::SystemRemoteDataPathsSource");
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
        // `shadow/../{part_name}/frozen_metadata.txt` is not an object metadata file
        const auto path = fs::path(local_path);
        return (path.filename() == "revision.txt" &&
                path.parent_path().has_parent_path() &&
                path.parent_path().parent_path().filename() == "shadow") ||
                path.filename() == "frozen_metadata.txt";
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

    /// plain_rewritable disks are traversed with the same lazy DFS, but starting from the disk root (not
    /// just store/data/shadow) so that blobs not reachable through the logical tree are surfaced too
    /// (e.g. leftovers of an interrupted removal). For these disks a few extra columns are filled in.
    bool current_disk_is_plain_rewritable = false;
    std::string plain_common_prefix;
    /// Metadata type name of the current disk, computed once per disk instead of per row.
    String current_metadata_type_name;
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
            std::make_shared<const Block>(header),
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

    /// Prune disks that cannot match the query's `disk_name` predicate, so we don't traverse every disk
    /// on the server (which has no pushdown and can be very expensive on instances with many disks).
    void applyFilters(ActionDAGNodes added_filter_nodes) override;

private:
    std::shared_ptr<const StorageLimitsList> storage_limits;
    const UInt64 max_block_size;
    DisksMap disks;
};


StorageSystemRemoteDataPaths::StorageSystemRemoteDataPaths(const StorageID & table_id_)
    : StorageWithCommonVirtualColumns(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription(
    {
        {"disk_name", std::make_shared<DataTypeString>(), "Disk name."},
        {"path", std::make_shared<DataTypeString>(), "Disk path."},
        {"metadata_type", std::make_shared<DataTypeString>(), "Metadata storage type of the disk (e.g. `Local`, `PlainRewritable`)."},
        {"cache_base_path", std::make_shared<DataTypeString>(), "Base directory of cache files."},
        {"local_path", std::make_shared<DataTypeString>(), "Path of ClickHouse file, also used as metadata path."},
        {"remote_path", std::make_shared<DataTypeString>(), "Blob path in object storage, with which ClickHouse file is associated with."},
        {"size", std::make_shared<DataTypeUInt64>(), "Size of the file (compressed)."},
        {"common_prefix_for_blobs", std::make_shared<DataTypeString>(), "Common prefix for blobs in object storage."},
        {"cache_paths", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Cache files for corresponding blob."},
        {"last_modified", std::make_shared<DataTypeDateTime>(), "Last modification time of the blob. Populated for `PlainRewritable` disks; zero otherwise."},
        {"is_ephemeral", std::make_shared<DataTypeUInt8>(),
            "For `PlainRewritable` disks, whether the blob looks like an ephemeral temporary object (also matches in-flight "
            "operations; only entries persisting across refreshes are leaks). Zero for other metadata types."},
    }));
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

VirtualColumnsDescription StorageSystemRemoteDataPaths::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}

void StorageSystemRemoteDataPaths::readImpl(
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
    auto header = storage_snapshot->metadata->getSampleBlockWithVirtuals(VirtualsKind::All, VirtualsMaterializationPlace::Reader);
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

void ReadFromSystemRemoteDataPaths::applyFilters(ActionDAGNodes added_filter_nodes)
{
    SourceStepWithFilter::applyFilters(std::move(added_filter_nodes));

    if (!filter_actions_dag)
        return;

    const auto * predicate = filter_actions_dag->getOutputs().at(0);
    if (!predicate)
        return;

    /// Build a block with all disk names and apply the query's `disk_name` predicate to it, so the source
    /// only traverses the matching disks. This avoids a full DFS over every disk on the server (there is
    /// no other pushdown), which is expensive on instances with many disks.
    auto disk_name_column = ColumnString::create();
    for (const auto & [disk_name, _] : disks)
        disk_name_column->insertData(disk_name.data(), disk_name.size());

    Block block{{std::move(disk_name_column), std::make_shared<DataTypeString>(), "disk_name"}};
    VirtualColumnUtils::filterBlockWithPredicate(predicate, block, context);

    std::unordered_set<std::string_view> allowed_disks;
    const auto & filtered_column = block.getByPosition(0).column;
    for (size_t i = 0; i < filtered_column->size(); ++i)
        allowed_disks.insert(filtered_column->getDataAt(i));

    std::erase_if(disks, [&](const auto & disk) { return !allowed_disks.contains(disk.first); });
}

void ReadFromSystemRemoteDataPaths::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & /*settings*/)
{
    auto source = std::make_shared<SystemRemoteDataPathsSource>(std::move(disks), getOutputHeader(), max_block_size, context);
    source->setStorageLimits(storage_limits);
    processors.emplace_back(source);
    pipeline.init(Pipe(std::move(source)));
}

bool SystemRemoteDataPathsSource::nextDisk()
{
    while (current_disk < static_cast<ssize_t>(disks.size()))
    {
        paths_stack.clear();
        current_disk_is_plain_rewritable = false;
        plain_common_prefix.clear();
        ++current_disk;

        if (current_disk >= static_cast<ssize_t>(disks.size()))
            break;

        const auto & disk = disks[current_disk].second;
        const auto metadata_type = disk->getDataSourceDescription().metadata_type;
        current_metadata_type_name = String{magic_enum::enum_name(metadata_type)};

        auto & current = paths_stack.emplace_back();

        if (metadata_type == MetadataStorageType::PlainRewritable)
        {
            /// plain_rewritable disks keep their layout in an in-memory tree without the usual
            /// store/data/shadow namespace, so traverse from the disk root. This also surfaces blobs
            /// not reachable through the logical tree (e.g. leftovers of an interrupted removal).
            current_disk_is_plain_rewritable = true;
            /// Take the prefix from the object storage contract rather than casting the metadata storage:
            /// cached/encrypted disks over plain_rewritable still report metadata_type = PlainRewritable but
            /// wrap the metadata storage, so a cast to the concrete type would miss them.
            auto object_storage = disk->getObjectStorage();
            chassert(object_storage);
            if (object_storage)
                plain_common_prefix = object_storage->getCommonKeyPrefix();

            /// Honor traverse_shadow_remote_data_paths for the frozen-data namespace, exactly like the
            /// non-plain branch: keep the root traversal for extra temporary/leftover roots, but skip the
            /// `shadow` root unless the setting is enabled, and apply skipPredicateForShadowDir when it is.
            const bool traverse_shadow = context->getSettingsRef()[Setting::traverse_shadow_remote_data_paths];
            std::vector<std::string> roots;
            disk->listFiles("", roots);
            for (const auto & root : roots)
            {
                if (root == "shadow")
                {
                    if (traverse_shadow)
                        current.names.push_back({root, skipPredicateForShadowDir});
                    continue;
                }
                current.names.push_back({root, nullptr});
            }
        }
        else
        {
            /// Add dirs that we want to traverse. It's ok if some of them don't exist because traversal logic handles
            /// cases when children of a directory get deleted while traversal is running.
            current.names.push_back({"store", nullptr});
            current.names.push_back({"data", nullptr});
            if (context->getSettingsRef()[Setting::traverse_shadow_remote_data_paths])
                current.names.push_back({"shadow", skipPredicateForShadowDir});
        }

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

            /// Stop if current path is a file
            if (disk->existsFile(current_path))
                return true;

            /// Files or directories can disappear due to concurrent operations
            if (!disk->existsFileOrDirectory(current_path))
                continue;

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
    auto component_guard = Coordination::setCurrentComponent("SystemRemoteDataPathsSource::generate");
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
    MutableColumnPtr col_metadata_type = ColumnString::create();
    MutableColumnPtr col_last_modified = ColumnUInt32::create();
    MutableColumnPtr col_is_ephemeral = ColumnUInt8::create();

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
                col_cache_paths->byteSize() +
                col_metadata_type->byteSize() +
                col_last_modified->byteSize() +
                col_is_ephemeral->byteSize();
            if (total_size > max_block_size)
                break;
        }

        const auto & [disk_name, disk] = disks[current_disk];
        const auto & base_path = disk->getPath();

        FileCachePtr cache;
        if (disk->supportsCache())
            cache = FileCacheFactory::instance().getByName(disk->getCacheName())->cache;

        const auto metadata_storage = disk->getMetadataStorage();
        const std::string local_path = getCurrentPath();

        const auto & skip_predicate = getCurrentSkipPredicate();
        if (skip_predicate && skip_predicate(local_path))
            continue;

        StoredObjects storage_objects;
        try
        {
            storage_objects = metadata_storage->getStorageObjects(local_path);
        }
        catch (Exception & e)
        {
            /// Unfortunately in rare cases it can happen when files disappear
            /// or can be empty in case of operation interruption (like cancelled metadata fetch)
            if (e.code() == ErrorCodes::FILE_DOESNT_EXIST ||
                e.code() == ErrorCodes::DIRECTORY_DOESNT_EXIST ||
                e.code() == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF ||
                e.code() == ErrorCodes::CANNOT_READ_ALL_DATA)
                continue;

            e.addMessage("While parsing file {}", local_path);
            throw;
        }

        /// Extra per-file columns for plain_rewritable disks; left at default for other metadata types.
        time_t last_modified = 0;
        bool is_ephemeral = false;
        if (current_disk_is_plain_rewritable)
        {
            if (auto ts = metadata_storage->getLastModifiedIfExists(local_path))
                last_modified = ts->epochTime();
            /// A blob is an ephemeral leftover of an interrupted operation when the top component of its
            /// logical path is a temporary name (the rename target used by move/unlink/removeRecursive).
            is_ephemeral = PlainRewritableLayout::looksLikeEphemeralName(
                std::string_view(local_path).substr(0, local_path.find('/')));
        }

        for (const auto & object : storage_objects)
        {
            ++row_count;
            col_disk_name->insert(disk_name);
            col_base_path->insert(base_path);
            if (cache)
                col_cache_base_path->insert(cache->getBasePath());
            else
                col_cache_base_path->insertDefault();
            col_local_path->insert(local_path);
            col_remote_path->insert(object.remote_path);
            col_size->insert(object.bytes_size);

            if (current_disk_is_plain_rewritable)
                col_namespace->insert(plain_common_prefix);
            else
                col_namespace->insertDefault();

            if (cache)
            {
                auto cache_paths = cache->tryGetCachePaths(FileCacheKey::fromPath(object.remote_path));
                col_cache_paths->insert(Array(cache_paths.begin(), cache_paths.end()));
            }
            else
            {
                col_cache_paths->insertDefault();
            }
            col_metadata_type->insert(current_metadata_type_name);
            col_last_modified->insert(static_cast<UInt32>(last_modified));
            col_is_ephemeral->insert(is_ephemeral);
        }
    }
    while (nextFile() || nextDisk());

    Columns res_columns;
    res_columns.emplace_back(std::move(col_disk_name));
    res_columns.emplace_back(std::move(col_base_path));
    res_columns.emplace_back(std::move(col_metadata_type));
    res_columns.emplace_back(std::move(col_cache_base_path));
    res_columns.emplace_back(std::move(col_local_path));
    res_columns.emplace_back(std::move(col_remote_path));
    res_columns.emplace_back(std::move(col_size));
    res_columns.emplace_back(std::move(col_namespace));
    res_columns.emplace_back(std::move(col_cache_paths));
    res_columns.emplace_back(std::move(col_last_modified));
    res_columns.emplace_back(std::move(col_is_ephemeral));

    UInt64 num_rows = res_columns.at(0)->size();
    Chunk chunk(std::move(res_columns), num_rows);

    return chunk;
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemRemoteDataPaths) }
