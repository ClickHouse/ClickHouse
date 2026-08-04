#include <Common/Clusters/ClusterMetadataDDLWorker.h>

#include <Common/Exception.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <base/scope_guard.h>

#include <fmt/format.h>

#include <algorithm>
#include <optional>
#include <utility>
#include <vector>

namespace ProfileEvents
{
    extern const Event ZooKeeperWatchTriggeredClusterMetadata;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int UNFINISHED;
}

namespace
{

constexpr auto DEFAULT_LOGS_TO_KEEP = "1000";
constexpr UInt32 SNAPSHOT_RELOAD_LAG_THRESHOLD = 1000;

struct BatchEntry
{
    String name;
    String finished_path;
    std::optional<ClusterMetadataMutation> mutation;
    bool needs_finish_status = false;
};

String joinPath(const String & left, const String & right)
{
    if (left.empty() || left == "/")
        return "/" + right;
    if (right.empty())
        return left;
    if (left.ends_with('/'))
        return left + right;
    return left + "/" + right;
}

void validateNodeName(const String & node_name)
{
    if (node_name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cluster metadata DDL worker node name cannot be empty");
    if (node_name.find('/') != String::npos)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cluster metadata DDL worker node name cannot contain '/': `{}`", node_name);
}

String getReplicaGroupRootOrThrow(const ClusterMetadataStoragePtr & storage)
{
    if (!storage)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ClusterMetadataDDLWorker requires non-null ClusterMetadataStorage");
    return storage->getReplicaGroupRoot();
}

}

ClusterMetadataDDLWorker::ClusterMetadataDDLWorker(
    ContextPtr context_,
    ClusterMetadataStoragePtr storage_,
    String node_name_,
    String zookeeper_name_,
    UInt32 max_log_entries_per_batch_,
    SnapshotReloader snapshot_reloader_,
    MutationPreparer mutation_preparer_,
    MutationApplier mutation_applier_)
    : DDLWorker(
        /* pool_size_ */ 1,
        joinPath(getReplicaGroupRootOrThrow(storage_), "log"),
        joinPath(getReplicaGroupRootOrThrow(storage_), "replicas"),
        context_,
        /* config */ nullptr,
        /* prefix */ {},
        zookeeper_name_,
        "ClusterMetadataDDLWorker")
    , storage(std::move(storage_))
    , node_name(std::move(node_name_))
    , snapshot_reloader(std::move(snapshot_reloader_))
    , mutation_preparer(std::move(mutation_preparer_))
    , mutation_applier(std::move(mutation_applier_))
    , max_log_entries_per_batch(max_log_entries_per_batch_)
{
    validateNodeName(node_name);
    if (max_log_entries_per_batch == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cluster metadata DDL worker max log entries per batch cannot be 0");

    replica_group_root = storage->getReplicaGroupRoot();
    log_root = queue_dir;
    counter_lock_path = joinPath(replica_group_root, "counter_lock");
    max_log_ptr_path = joinPath(replica_group_root, "max_log_ptr");
    logs_to_keep_path = joinPath(replica_group_root, "logs_to_keep");
    replicas_root = replicas_dir;
    replica_path = joinPath(replicas_root, node_name);
    replica_log_ptr_path = joinPath(replica_path, "log_ptr");
    replica_digest_path = joinPath(replica_path, "digest");
    replica_active_path = joinPath(replica_path, "active");
}

ClusterMetadataDDLWorker::~ClusterMetadataDDLWorker()
{
    ClusterMetadataDDLWorker::shutdown();
}

void ClusterMetadataDDLWorker::shutdown()
{
    DDLWorker::shutdown();

    auto component_guard = Coordination::setCurrentComponent("ClusterMetadataDDLWorker::shutdown");
    if (active_node_holder_zookeeper && !active_node_holder_zookeeper->expired())
        active_node_holder_zookeeper->tryRemove(replica_active_path);

    if (active_node_holder)
        active_node_holder->setAlreadyRemoved();
    active_node_holder.reset();
    active_node_holder_zookeeper.reset();
}

UInt32 ClusterMetadataDDLWorker::getLogPointer() const
{
    return readUInt32Node(getZooKeeperFromContext(), replica_log_ptr_path);
}

UInt32 ClusterMetadataDDLWorker::getMaxLogPointer() const
{
    return readUInt32Node(getZooKeeperFromContext(), max_log_ptr_path);
}

bool ClusterMetadataDDLWorker::processCommittedEntries()
{
    auto component_guard = Coordination::setCurrentComponent("ClusterMetadataDDLWorker::processCommittedEntries");
    return catchUpLocalSnapshot(getMaxLogPointer());
}

bool ClusterMetadataDDLWorker::catchUpLocalSnapshot(UInt32 target_log_ptr)
{
    std::lock_guard lock(processing_mutex);
    return catchUpLocalSnapshotUnlocked(target_log_ptr);
}

bool ClusterMetadataDDLWorker::catchUpLocalSnapshotUnlocked(UInt32 target_log_ptr)
{
    UInt32 log_ptr = getLogPointer();
    if (log_ptr >= target_log_ptr)
        return false;

    if (reloadSnapshotAndAdvanceIfTooFarBehind(log_ptr, target_log_ptr, SNAPSHOT_RELOAD_LAG_THRESHOLD))
        return true;

    bool made_progress = false;
    while (!stop_flag && log_ptr < target_log_ptr)
    {
        const UInt32 batch_end = std::min(target_log_ptr, log_ptr + max_log_entries_per_batch);
        const UInt32 processed_log_ptr = processEntriesBatch(log_ptr + 1, batch_end);
        if (processed_log_ptr <= log_ptr)
            break;

        log_ptr = processed_log_ptr;
        made_progress = true;
    }

    if (log_ptr < target_log_ptr)
        throw Exception(
            ErrorCodes::UNFINISHED,
            "Cannot prepare cluster metadata mutation because local snapshot is not caught up: log_ptr={}, target_log_ptr={}",
            log_ptr,
            target_log_ptr);

    return made_progress;
}

ClusterMetadataDDLWorker::EnqueuedMutationInfo ClusterMetadataDDLWorker::enqueueMutationForSync(const ClusterMetadataMutation & mutation)
{
    const auto enqueued = enqueueMutationImpl(mutation);
    if (enqueued.is_noop)
    {
        return EnqueuedMutationInfo{
            .entry_path = {},
            .replicas_path = replicas_dir,
            .zookeeper_name = zookeeper_name,
            .hosts_to_wait = {},
            .is_noop = true,
        };
    }

    auto zookeeper = getZooKeeperFromContext();
    auto hosts_to_wait = zookeeper->getChildren(replicas_root);
    std::sort(hosts_to_wait.begin(), hosts_to_wait.end());
    return EnqueuedMutationInfo{
        .entry_path = enqueued.entry_path,
        .replicas_path = replicas_dir,
        .zookeeper_name = zookeeper_name,
        .hosts_to_wait = std::move(hosts_to_wait),
    };
}

ClusterMetadataDDLWorker::EnqueuedMutation ClusterMetadataDDLWorker::enqueueMutationImpl(const ClusterMetadataMutation & mutation)
{
    auto component_guard = Coordination::setCurrentComponent("ClusterMetadataDDLWorker::enqueueMutationImpl");
    auto zookeeper = getZooKeeperFromContext();

    /// Acquire the global enqueue lock (ephemeral node, auto-released on session loss) so that
    /// reading `max_log_ptr` and creating the next `query-N` entry are serialized across all
    /// initiators in the replica group.
    const auto lock_code = zookeeper->tryCreate(counter_lock_path, node_name, zkutil::CreateMode::Ephemeral);
    if (lock_code == Coordination::Error::ZNODEEXISTS)
        throw Exception(
            ErrorCodes::UNFINISHED,
            "Cannot enqueue cluster metadata mutation because another node is allocating a log entry. Client should retry");
    if (lock_code != Coordination::Error::ZOK)
        throw zkutil::KeeperException::fromPath(lock_code, counter_lock_path);
    SCOPE_EXIT({
        try
        {
            zookeeper->tryRemove(counter_lock_path);
        }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format("Failed to release cluster metadata enqueue lock {}", counter_lock_path));
        }
    });

    EnqueuedMutation enqueued;
    {
        std::lock_guard processing_lock(processing_mutex);
        const UInt32 max_log_ptr = getMaxLogPointer();
        catchUpLocalSnapshotUnlocked(max_log_ptr);

        const UInt32 entry_number = max_log_ptr + 1;
        const String entry_name = DDLTaskBase::getLogEntryName(entry_number);
        const String entry_path = joinPath(log_root, entry_name);
        if (!mutation_preparer)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cluster metadata DDL worker cannot prepare entries without mutation preparer");
        const auto prepared_mutation = mutation_preparer(mutation);
        if (prepared_mutation.is_noop)
        {
            LOG_DEBUG(
                log,
                "Skipping enqueue for cluster metadata mutation {} on `{}` after catch-up (IF EXISTS / IF NOT EXISTS no-op)",
                static_cast<unsigned>(mutation.type),
                mutation.name);
            return EnqueuedMutation{
                .entry_path = {},
                .entry_name = {},
                .entry_number = 0,
                .digest = {},
                .is_noop = true,
            };
        }

        enqueued = EnqueuedMutation{
            .entry_path = entry_path,
            .entry_name = entry_name,
            .entry_number = entry_number,
            .digest = prepared_mutation.digest,
        };

        /// Allocate the entry number, create the whole log entry, mark this initiator as finished,
        /// advance `max_log_ptr` and release the lock in a single atomic transaction. If anything fails
        /// (e.g. a metadata node already exists), nothing is persisted: no `query-N` node is left behind
        /// and `max_log_ptr` is not advanced, so the strictly-sequential consumer never observes a gap
        /// that would wedge the queue.
        const String status = ExecutionStatus(0).serializeText();
        const String finished_path = joinPath(joinPath(entry_path, "finished"), node_name);
        Coordination::Requests ops;
        appendMutationOps(ops, prepared_mutation.metadata_mutation);
        storage->appendWriteSnapshotDigestOps(ops, prepared_mutation.digest);
        ops.emplace_back(zkutil::makeCreateRequest(entry_path, "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(joinPath(entry_path, "entry"), storage->encodeData(mutation.serialize()), zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(joinPath(entry_path, "finished"), "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeCreateRequest(joinPath(entry_path, "active"), "", zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeSetRequest(replica_digest_path, prepared_mutation.digest, -1));
        ops.emplace_back(zkutil::makeCreateRequest(finished_path, status, zkutil::CreateMode::Persistent));
        ops.emplace_back(zkutil::makeSetRequest(replica_log_ptr_path, toString(entry_number), -1));
        ops.emplace_back(zkutil::makeSetRequest(max_log_ptr_path, toString(entry_number), -1));
        ops.emplace_back(zkutil::makeRemoveRequest(counter_lock_path, -1));
        zookeeper->multi(ops);

        applyEnqueuedMutationLocallyUnlocked(enqueued, mutation);
    }

    LOG_DEBUG(log, "Enqueued cluster metadata mutation {} at {}", static_cast<unsigned>(mutation.type), enqueued.entry_path);
    return enqueued;
}

void ClusterMetadataDDLWorker::enqueueMutationAndConfirmLocal(const ClusterMetadataMutation & mutation)
{
    const auto enqueued = enqueueMutationImpl(mutation);
    if (enqueued.is_noop)
        return;

    auto zookeeper = getZooKeeperFromContext();
    const String finished_path = joinPath(joinPath(enqueued.entry_path, "finished"), node_name);
    String status_data;
    if (!zookeeper->tryGet(finished_path, status_data))
        throw Exception(ErrorCodes::UNFINISHED, "Cluster metadata mutation {} has not been applied by node `{}`", enqueued.entry_name, node_name);

    const auto status = ExecutionStatus::fromText(status_data);
    if (status.code != 0)
        throw Exception(status.code, "Cluster metadata mutation {} failed on node `{}`: {}", enqueued.entry_name, node_name, status.message);
}

void ClusterMetadataDDLWorker::applyEnqueuedMutationLocallyUnlocked(const EnqueuedMutation & enqueued, const ClusterMetadataMutation & mutation)
{
    auto component_guard = Coordination::setCurrentComponent("ClusterMetadataDDLWorker::applyEnqueuedMutationLocally");
    if (!mutation_applier)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cluster metadata DDL worker cannot apply entries without mutation applier");

    const String local_digest = mutation_applier(std::vector<ClusterMetadataMutation>{mutation});
    if (local_digest != enqueued.digest)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cluster metadata DDL worker local digest {} does not match just-enqueued durable digest {}",
            local_digest,
            enqueued.digest);
}

void ClusterMetadataDDLWorker::scheduleTasks(bool /*reinitialized*/)
{
    auto component_guard = Coordination::setCurrentComponent("ClusterMetadataDDLWorker::scheduleTasks");
    auto zookeeper = getZooKeeper();
    Coordination::Stat stat;
    zookeeper->getChildrenWatch(
        queue_dir,
        &stat,
        Coordination::WatchCallbackPtrOrEventPtr{queue_updated_event, ProfileEvents::ZooKeeperWatchTriggeredClusterMetadata});

    const UInt32 log_ptr = getLogPointer();
    const UInt32 max_log_ptr = getMaxLogPointer();
    const UInt32 logs_to_keep = readUInt32Node(zookeeper, logs_to_keep_path);
    if (reloadSnapshotAndAdvanceIfTooFarBehind(log_ptr, max_log_ptr, logs_to_keep))
        return;

    if (log_ptr < max_log_ptr)
    {
        if (!processCommittedEntries())
        {
            const String entry_data_path = joinPath(joinPath(queue_dir, DDLTaskBase::getLogEntryName(log_ptr + 1)), "entry");
            zookeeper->exists(entry_data_path, nullptr, queue_updated_event);
        }
    }
    else
        LOG_DEBUG(log, "No cluster metadata DDL entries to process");
}

void ClusterMetadataDDLWorker::initializeReplication()
{
    auto component_guard = Coordination::setCurrentComponent("ClusterMetadataDDLWorker::initializeReplication");
    initKeeperLayout();
    registerReplica();

    const UInt32 log_ptr = getLogPointer();
    const UInt32 max_log_ptr = getMaxLogPointer();
    const UInt32 logs_to_keep = readUInt32Node(getZooKeeper(), logs_to_keep_path);

    reloadSnapshotAndAdvanceIfTooFarBehind(log_ptr, max_log_ptr, logs_to_keep);

    LOG_INFO(log, "Cluster metadata DDL worker initialized for node `{}` at `{}`", node_name, replica_group_root);
}

void ClusterMetadataDDLWorker::initKeeperLayout()
{
    auto zookeeper = getZooKeeper();
    storage->initLayout();
    zookeeper->createIfNotExists(log_root, "");
    zookeeper->createIfNotExists(logs_to_keep_path, DEFAULT_LOGS_TO_KEEP);
    initializeCounter();
}

void ClusterMetadataDDLWorker::initializeCounter()
{
    auto zookeeper = getZooKeeper();
    /// `max_log_ptr` holds the highest allocated log entry number. 0 means "no entries yet", so the
    /// first enqueued entry is `query-0000000001`. Entry numbers are derived from `max_log_ptr` while
    /// holding `counter_lock` (see `enqueueMutationImpl`), so no separate sequential `counter` subtree is
    /// needed and there is no allocate-then-commit window that could leak a gap.
    zookeeper->createIfNotExists(max_log_ptr_path, "0");
}

void ClusterMetadataDDLWorker::registerReplica()
{
    auto zookeeper = getZooKeeper();
    zookeeper->createIfNotExists(replicas_root, "");
    /// Keep the leaf name as ServerUUID (stable identity for finished/ + log_ptr), but store a
    /// human-readable `host:port` as node data so SYNC status can display it.
    zookeeper->createIfNotExists(replica_path, host_fqdn_id);
    zookeeper->createOrUpdate(replica_path, host_fqdn_id, zkutil::CreateMode::Persistent);
    zookeeper->createIfNotExists(replica_log_ptr_path, "0");
    zookeeper->createIfNotExists(replica_digest_path, "0");
}

bool ClusterMetadataDDLWorker::reloadSnapshotAndAdvanceIfTooFarBehind(UInt32 log_ptr, UInt32 max_log_ptr, UInt32 logs_to_keep)
{
    if (log_ptr >= max_log_ptr || max_log_ptr - log_ptr <= logs_to_keep)
        return false;

    LOG_WARNING(
        log,
        "Cluster metadata DDL worker node `{}` is too far behind: log_ptr={}, max_log_ptr={}, logs_to_keep={}. "
        "Reloading snapshot and advancing log_ptr.",
        node_name,
        log_ptr,
        max_log_ptr,
        logs_to_keep);

    if (!snapshot_reloader)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cluster metadata DDL worker cannot reload snapshot because snapshot reloader is not set");

    const String digest = snapshot_reloader();
    updateReplicaDigest(digest);
    updateReplicaLogPointer(max_log_ptr);
    return true;
}

void ClusterMetadataDDLWorker::appendMutationOps(Coordination::Requests & ops, const ClusterMetadataMutation & mutation) const
{
    switch (mutation.type)
    {
        case ClusterMetadataMutation::Type::CreateEndpoint:
            storage->appendCreateEndpointOps(ops, mutation.name, EndpointCatalogDefinition::deserialize(mutation.definition_data));
            return;
        case ClusterMetadataMutation::Type::DropEndpoint:
            storage->appendDropEndpointOps(ops, mutation.name);
            return;
        case ClusterMetadataMutation::Type::AlterEndpoint:
            storage->appendUpsertEndpointOps(ops, mutation.name, EndpointCatalogDefinition::deserialize(mutation.definition_data));
            return;
        case ClusterMetadataMutation::Type::CreateShard:
            storage->appendCreateShardOps(ops, mutation.name, ShardCatalogDefinition::deserialize(mutation.definition_data));
            return;
        case ClusterMetadataMutation::Type::DropShard:
            storage->appendDropShardOps(ops, mutation.name);
            return;
        case ClusterMetadataMutation::Type::AlterShard:
            storage->appendUpsertShardOps(ops, mutation.name, ShardCatalogDefinition::deserialize(mutation.definition_data));
            return;
        case ClusterMetadataMutation::Type::CreateCluster:
            storage->appendCreateClusterOps(ops, mutation.name, ClusterCatalogDefinition::deserialize(mutation.definition_data));
            return;
        case ClusterMetadataMutation::Type::DropCluster:
            storage->appendDropClusterOps(ops, mutation.name);
            return;
        case ClusterMetadataMutation::Type::AlterCluster:
            storage->appendUpsertClusterOps(ops, mutation.name, ClusterCatalogDefinition::deserialize(mutation.definition_data));
            return;
        case ClusterMetadataMutation::Type::ModifyEndpointProperties:
        case ClusterMetadataMutation::Type::ModifyShardProperties:
        case ClusterMetadataMutation::Type::AddShardReplicas:
        case ClusterMetadataMutation::Type::DropShardReplicas:
        case ClusterMetadataMutation::Type::ReplaceShardReplicas:
        case ClusterMetadataMutation::Type::AddClusterMembers:
        case ClusterMetadataMutation::Type::DropClusterMembers:
        case ClusterMetadataMutation::Type::ReplaceClusterMembers:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cluster metadata intent mutation must be prepared before writing metadata to Keeper");
    }
}

void ClusterMetadataDDLWorker::markReplicasActive(bool /*reinitialized*/)
{
    auto component_guard = Coordination::setCurrentComponent("ClusterMetadataDDLWorker::markReplicasActive");
    if (active_node_holder && active_node_holder_zookeeper && !active_node_holder_zookeeper->expired())
        return;

    auto zookeeper = getZooKeeper();
    /// Refresh address metadata in case host/port changed after an upgrade or restart.
    zookeeper->createOrUpdate(replica_path, host_fqdn_id, zkutil::CreateMode::Persistent);

    const String active_id = node_name;
    zookeeper->createAncestors(replica_active_path);
    zookeeper->deleteEphemeralNodeIfContentMatches(replica_active_path, active_id);

    if (active_node_holder)
        active_node_holder->setAlreadyRemoved();
    active_node_holder.reset();

    zookeeper->create(replica_active_path, active_id, zkutil::CreateMode::Ephemeral);
    active_node_holder_zookeeper = zookeeper;
    active_node_holder = zkutil::EphemeralNodeHolder::existing(replica_active_path, *active_node_holder_zookeeper);
}

UInt32 ClusterMetadataDDLWorker::processEntriesBatch(UInt32 first_entry, UInt32 last_entry)
{
    auto component_guard = Coordination::setCurrentComponent("ClusterMetadataDDLWorker::processEntriesBatch");
    auto zookeeper = getZooKeeper();

    bool local_materialization_started = false;
    bool local_snapshot_published = false;
    bool writing_success_status = false;
    String failure_entry_name = DDLTaskBase::getLogEntryName(first_entry);
    String failure_finished_path = joinPath(joinPath(joinPath(log_root, failure_entry_name), "finished"), node_name);
    try
    {
        std::vector<BatchEntry> entries;
        entries.reserve(last_entry - first_entry + 1);

        std::vector<String> entry_names;
        std::vector<String> entry_paths;
        std::vector<String> finished_paths;
        entry_names.reserve(last_entry - first_entry + 1);
        entry_paths.reserve(last_entry - first_entry + 1);
        finished_paths.reserve(last_entry - first_entry + 1);

        for (UInt32 entry_number = first_entry; entry_number <= last_entry; ++entry_number)
        {
            entry_names.push_back(DDLTaskBase::getLogEntryName(entry_number));
            entry_paths.push_back(joinPath(log_root, entry_names.back()));
            finished_paths.push_back(joinPath(joinPath(entry_paths.back(), "finished"), node_name));
        }

        auto finished_responses = zookeeper->tryGet(finished_paths);

        std::vector<std::pair<size_t, size_t>> entries_to_read;
        entries_to_read.reserve(entry_names.size());
        UInt32 last_ready_entry = first_entry - 1;
        for (size_t i = 0; i < entry_names.size(); ++i)
        {
            failure_entry_name = entry_names[i];
            failure_finished_path = finished_paths[i];

            const auto & finished_response = finished_responses[i];
            if (finished_response.error == Coordination::Error::ZOK)
            {
                const auto status = ExecutionStatus::fromText(finished_response.data);
                if (status.code != 0)
                    break;

                entries.push_back(BatchEntry{
                    .name = entry_names[i],
                    .finished_path = finished_paths[i],
                    .mutation = std::nullopt,
                    .needs_finish_status = false,
                });
                last_ready_entry = first_entry + static_cast<UInt32>(i);
                continue;
            }

            if (finished_response.error != Coordination::Error::ZNONODE)
                throw zkutil::KeeperException::fromPath(finished_response.error, finished_paths[i]);

            entries_to_read.emplace_back(i, entries.size());
            entries.push_back(BatchEntry{
                .name = entry_names[i],
                .finished_path = finished_paths[i],
                .mutation = std::nullopt,
                .needs_finish_status = true,
            });
            last_ready_entry = first_entry + static_cast<UInt32>(i);
        }

        if (!entries_to_read.empty())
        {
            std::vector<String> entry_data_paths;
            entry_data_paths.reserve(entries_to_read.size());
            for (const auto & entry_to_read : entries_to_read)
                entry_data_paths.push_back(joinPath(entry_paths[entry_to_read.first], "entry"));

            auto entry_data_responses = zookeeper->tryGet(entry_data_paths);
            for (size_t response_index = 0; response_index < entry_data_responses.size(); ++response_index)
            {
                const auto entry_index = entries_to_read[response_index].first;
                failure_entry_name = entry_names[entry_index];
                failure_finished_path = finished_paths[entry_index];

                if (entry_data_responses[response_index].error != Coordination::Error::ZOK)
                    throw zkutil::KeeperException::fromPath(entry_data_responses[response_index].error, entry_data_paths[response_index]);

                auto mutation = ClusterMetadataMutation::deserialize(
                    storage->decodeData(entry_data_responses[response_index].data));
                entries[entries_to_read[response_index].second].mutation = std::move(mutation);
            }
        }

        if (entries.empty())
            return first_entry - 1;

        const bool caught_up_to_durable_log = last_ready_entry >= getMaxLogPointer();
        const String target_digest = caught_up_to_durable_log ? storage->readSnapshotDigest() : "";
        String local_digest = zookeeper->get(replica_digest_path);
        std::vector<ClusterMetadataMutation> mutations_to_apply;
        mutations_to_apply.reserve(entries_to_read.size());
        for (const auto & entry : entries)
        {
            if (entry.mutation)
                mutations_to_apply.push_back(*entry.mutation);
        }

        if (!mutations_to_apply.empty())
        {
            if (!mutation_applier)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Cluster metadata DDL worker cannot apply entries without mutation applier");

            local_materialization_started = true;
            local_digest = mutation_applier(mutations_to_apply);
            if (caught_up_to_durable_log && local_digest != target_digest)
            {
                if (!snapshot_reloader)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Cluster metadata DDL worker local digest {} does not match durable digest {} and snapshot reloader is not set",
                        local_digest,
                        target_digest);

                local_digest = snapshot_reloader();
                if (local_digest != target_digest)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Cluster metadata DDL worker reloaded digest {} does not match durable digest {}",
                        local_digest,
                        target_digest);
            }
            local_snapshot_published = true;
        }

        const String status = ExecutionStatus(0).serializeText();
        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeSetRequest(replica_digest_path, local_digest, -1));
        for (const auto & entry : entries)
        {
            if (entry.needs_finish_status)
                ops.emplace_back(zkutil::makeCreateRequest(entry.finished_path, status, zkutil::CreateMode::Persistent));
        }
        ops.emplace_back(zkutil::makeSetRequest(replica_log_ptr_path, toString(last_ready_entry), -1));
        writing_success_status = true;
        zookeeper->multi(ops);

        LOG_DEBUG(log, "Applied cluster metadata DDL log entries from {} to {}", entries.front().name, entries.back().name);
        return last_ready_entry;
    }
    catch (...)
    {
        tryLogCurrentException(
            log,
            fmt::format("Incremental apply of cluster metadata DDL log entry {} failed", failure_entry_name));

        if (local_materialization_started || local_snapshot_published || writing_success_status)
            return first_entry - 1;

        const String status = ExecutionStatus::fromCurrentException().serializeText();
        try
        {
            zookeeper->createOrUpdate(failure_finished_path, status, zkutil::CreateMode::Persistent);
        }
        catch (...)
        {
            tryLogCurrentException(
                log,
                fmt::format("Failed to write failure status for cluster metadata DDL log entry {} to Keeper", failure_entry_name));
        }
        return first_entry - 1;
    }
}

void ClusterMetadataDDLWorker::updateReplicaDigest(const String & digest)
{
    auto zookeeper = getZooKeeper();
    storage->writeSnapshotDigest(digest);
    zookeeper->createOrUpdate(replica_digest_path, digest, zkutil::CreateMode::Persistent);
}

bool ClusterMetadataDDLWorker::canRemoveEntry(UInt32 entry_number) const
{
    auto zookeeper = getZooKeeper();
    const auto replicas = zookeeper->getChildren(replicas_root);
    for (const auto & replica : replicas)
    {
        const String current_replica_path = joinPath(replicas_root, replica);
        if (!zookeeper->exists(joinPath(current_replica_path, "active")))
            continue;

        const String log_ptr_data = zookeeper->get(joinPath(current_replica_path, "log_ptr"));
        if (parse<UInt32>(log_ptr_data) <= entry_number)
            return false;
    }
    return true;
}

bool ClusterMetadataDDLWorker::canRemoveQueueEntry(const String & entry_name, const Coordination::Stat & /*stat*/)
{
    const UInt32 max_log_ptr = getMaxLogPointer();
    const UInt32 logs_to_keep = readUInt32Node(getZooKeeper(), logs_to_keep_path);
    const UInt32 entry_number = logEntryNumber(entry_name);
    return entry_number + logs_to_keep < max_log_ptr && canRemoveEntry(entry_number);
}

UInt32 ClusterMetadataDDLWorker::readUInt32Node(const ZooKeeperPtr & zookeeper, const String & path) const
{
    const String data = zookeeper->get(path);
    if (data.empty())
        return 0;
    return parse<UInt32>(data);
}

void ClusterMetadataDDLWorker::updateReplicaLogPointer(UInt32 log_pointer)
{
    auto zookeeper = getZooKeeper();
    zookeeper->createOrUpdate(replica_log_ptr_path, toString(log_pointer), zkutil::CreateMode::Persistent);
}

UInt32 ClusterMetadataDDLWorker::logEntryNumber(const String & entry_name) const
{
    constexpr std::string_view prefix = "query-";
    if (!entry_name.starts_with(prefix))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected cluster metadata DDL log entry name `{}`", entry_name);
    return parse<UInt32>(entry_name.substr(prefix.size()));
}

}
