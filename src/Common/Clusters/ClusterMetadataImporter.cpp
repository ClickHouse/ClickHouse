#include <Common/Clusters/ClusterMetadataImporter.h>

#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>

#include <fmt/format.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_CLUSTER_DEFINITION;
    extern const int LOGICAL_ERROR;
}

namespace
{

String joinKeeperPath(String root_path, const String & child)
{
    if (root_path.empty())
        root_path = "/";
    if (root_path.front() != '/')
        root_path = "/" + root_path;
    while (root_path.size() > 1 && root_path.ends_with('/'))
        root_path.pop_back();
    if (child.empty())
        return root_path;
    return (fs::path(root_path) / child).string();
}

void resolveShardEndpoints(
    ShardCatalogDefinition & shard,
    const std::unordered_map<String, EndpointCatalogDefinition> & endpoints)
{
    shard.endpoints.clear();
    shard.endpoints.reserve(shard.endpoint_names.size());
    for (const auto & endpoint_name : shard.endpoint_names)
    {
        const auto it = endpoints.find(endpoint_name);
        if (it == endpoints.end())
            throw Exception(ErrorCodes::BAD_CLUSTER_DEFINITION, "Cluster metadata endpoint `{}` does not exist", endpoint_name);
        shard.endpoints.push_back(it->second);
    }
}

}

ClusterMetadataImporter::ClusterMetadataImporter(
    ContextPtr context_,
    String keeper_name_,
    String root_path_,
    bool encrypted_,
    String encryption_key_hex_,
    String encryption_algorithm_,
    std::vector<String> replica_groups_,
    PublishCallback publish_callback_)
    : context(std::move(context_))
    , keeper_name(std::move(keeper_name_))
    , root_path(joinKeeperPath(std::move(root_path_), ""))
    , encrypted(encrypted_)
    , encryption_key_hex(std::move(encryption_key_hex_))
    , encryption_algorithm(std::move(encryption_algorithm_))
    , replica_groups(std::move(replica_groups_))
    , publish_callback(std::move(publish_callback_))
{
    if (!context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterMetadataImporter requires non-null Context");
}

ClusterMetadataImporter::~ClusterMetadataImporter()
{
    shutdown();
}

void ClusterMetadataImporter::startup()
{
    if (replica_groups.empty())
        return;

    initializeGroups();

    std::lock_guard lock(mutex);
    if (shutdown_called)
        return;

    task = context->getSchedulePool().createTask(
        StorageID::createEmpty(), "ClusterMetadataImporter", [this] { refreshTask(); });
    task->activateAndSchedule();
}

void ClusterMetadataImporter::shutdown()
{
    BackgroundSchedulePool::TaskHolder task_to_stop;
    {
        std::lock_guard lock(mutex);
        shutdown_called = true;
        task_to_stop = std::move(task);
    }

    /// `deactivate` can wait for the current run to finish. Do it outside `mutex` because the task
    /// itself takes `mutex` while copying/importing state.
    if (task_to_stop)
        task_to_stop->deactivate();

    std::lock_guard lock(mutex);
    imported_groups.clear();
}

std::vector<ClusterMetadataImporter::ImportedSnapshot> ClusterMetadataImporter::getLoadedSnapshots() const
{
    std::lock_guard lock(mutex);

    std::vector<ImportedSnapshot> snapshots;
    snapshots.reserve(imported_groups.size());
    for (const auto & group : imported_groups)
    {
        if (group.loaded)
            snapshots.push_back(ImportedSnapshot{group.replica_group, group.snapshot});
    }
    return snapshots;
}

void ClusterMetadataImporter::initializeGroups()
{
    auto zookeeper = context->getDefaultOrAuxiliaryZooKeeper(keeper_name);

    std::lock_guard lock(mutex);
    imported_groups.clear();
    imported_groups.reserve(replica_groups.size());
    for (const auto & replica_group : replica_groups)
    {
        const String imported_root = joinKeeperPath(root_path, replica_group);
        auto imported_storage = std::make_shared<ClusterMetadataStorage>(
            zookeeper,
            imported_root,
            encrypted,
            encryption_key_hex,
            encryption_algorithm);
        imported_groups.push_back(ImportedGroup{replica_group, std::move(imported_storage), {}, "", false});
        LOG_INFO(log, "Will import read-only cluster metadata from replica group `{}`", replica_group);
    }
}

void ClusterMetadataImporter::refreshTask()
{
    {
        std::lock_guard lock(mutex);
        if (shutdown_called)
            return;
    }

    try
    {
        if (refreshSnapshots() && publish_callback)
            publish_callback();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to refresh imported cluster metadata");
    }

    std::lock_guard lock(mutex);
    if (!shutdown_called && task)
        task->scheduleAfter(REFRESH_INTERVAL_MS);
}

bool ClusterMetadataImporter::refreshSnapshots()
{
    auto component_guard = Coordination::setCurrentComponent("ClusterMetadataImporter::refreshSnapshots");

    size_t group_count = 0;
    {
        std::lock_guard lock(mutex);
        group_count = imported_groups.size();
    }

    bool changed = false;
    for (size_t i = 0; i < group_count; ++i)
    {
        ClusterMetadataStoragePtr group_storage;
        String last_digest;
        bool was_loaded = false;
        String group_name;
        {
            std::lock_guard lock(mutex);
            if (i >= imported_groups.size())
                break;
            group_storage = imported_groups[i].storage;
            last_digest = imported_groups[i].digest;
            was_loaded = imported_groups[i].loaded;
            group_name = imported_groups[i].replica_group;
        }

        if (!group_storage)
            continue;

        try
        {
            /// The importing node never creates the imported group's layout; if it does not exist yet
            /// there is simply nothing to import.
            if (!group_storage->metadataInitialized())
                continue;

            const String current_digest = group_storage->readSnapshotDigest();
            if (was_loaded && current_digest == last_digest)
                continue;

            ClusterMetadataStorage::Snapshot new_snapshot = group_storage->readSnapshot();
            for (auto & [shard_name, shard] : new_snapshot.shards)
            {
                if (shard.name.empty())
                    shard.name = shard_name;
                resolveShardEndpoints(shard, new_snapshot.endpoints);
            }

            {
                std::lock_guard lock(mutex);
                if (i >= imported_groups.size())
                    break;
                imported_groups[i].snapshot = std::move(new_snapshot);
                imported_groups[i].digest = current_digest;
                imported_groups[i].loaded = true;
            }
            changed = true;
            LOG_INFO(log, "Loaded imported cluster metadata from replica group `{}`", group_name);
        }
        catch (...)
        {
            tryLogCurrentException(
                log, fmt::format("Failed to load imported cluster metadata from replica group `{}`", group_name));
        }
    }

    return changed;
}

}
