#include <Common/Clusters/ClusterMetadataDDLWorker.h>

#include <Common/Exception.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <base/scope_guard.h>

#include <fmt/format.h>

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
    SnapshotReloader snapshot_reloader_,
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
    , mutation_applier(std::move(mutation_applier_))
{
    validateNodeName(node_name);

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
    shutdown();
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

bool ClusterMetadataDDLWorker::processCommittedEntriesOnce()
{
    auto component_guard = Coordination::setCurrentComponent("ClusterMetadataDDLWorker::processCommittedEntriesOnce");
    std::lock_guard lock(processing_mutex);

    UInt32 log_ptr = getLogPointer();
    const UInt32 max_log_ptr = getMaxLogPointer();
    if (log_ptr >= max_log_ptr)
        return false;

    bool made_progress = false;
    while (!stop_flag && log_ptr < max_log_ptr)
    {
        const UInt32 next_entry = log_ptr + 1;
        if (!processEntry(next_entry))
            break;

        log_ptr = next_entry;
        made_progress = true;
    }

    return made_progress;
}

String ClusterMetadataDDLWorker::enqueueMutation(const ClusterMetadataMutation & mutation)
{
    auto component_guard = Coordination::setCurrentComponent("ClusterMetadataDDLWorker::enqueueMutation");
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
    SCOPE_EXIT({ zookeeper->tryRemove(counter_lock_path); });

    const UInt32 entry_number = getMaxLogPointer() + 1;
    const String entry_name = DDLTaskBase::getLogEntryName(entry_number);
    const String entry_path = joinPath(log_root, entry_name);

    /// Allocate the entry number, create the whole log entry, advance `max_log_ptr` and release the
    /// lock in a single atomic transaction. If anything fails (e.g. a metadata node already exists),
    /// nothing is persisted: no `query-N` node is left behind and `max_log_ptr` is not advanced, so
    /// the strictly-sequential consumer never observes a gap that would wedge the queue.
    Coordination::Requests ops;
    appendMutationOps(ops, mutation);
    ops.emplace_back(zkutil::makeCreateRequest(entry_path, "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(joinPath(entry_path, "entry"), storage->encodePayloadForKeeper(mutation.serialize()), zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(joinPath(entry_path, "committed"), node_name, zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(joinPath(entry_path, "active"), "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeCreateRequest(joinPath(entry_path, "finished"), "", zkutil::CreateMode::Persistent));
    ops.emplace_back(zkutil::makeSetRequest(max_log_ptr_path, toString(entry_number), -1));
    ops.emplace_back(zkutil::makeRemoveRequest(counter_lock_path, -1));
    zookeeper->multi(ops);

    queue_updated_event->set();
    LOG_DEBUG(log, "Enqueued cluster metadata mutation {} at {}", static_cast<unsigned>(mutation.type), entry_path);
    return entry_path;
}

void ClusterMetadataDDLWorker::enqueueMutationAndWait(const ClusterMetadataMutation & mutation)
{
    const String entry_path = enqueueMutation(mutation);
    const String entry_name = entry_path.substr(entry_path.rfind('/') + 1);
    const UInt32 entry_number = logEntryNumber(entry_name);
    processCommittedEntriesOnce();

    auto zookeeper = getZooKeeperFromContext();
    const String finished_path = joinPath(joinPath(entry_path, "finished"), node_name);
    String status_data;
    if (!zookeeper->tryGet(finished_path, status_data))
        throw Exception(ErrorCodes::UNFINISHED, "Cluster metadata mutation {} has not been applied by node `{}`", entry_name, node_name);

    const auto status = ExecutionStatus::fromText(status_data);
    if (status.code != 0)
        throw Exception(status.code, "Cluster metadata mutation {} failed on node `{}`: {}", entry_name, node_name, status.message);

    setLogPointer(entry_number);
}

void ClusterMetadataDDLWorker::scheduleTasks(bool /*reinitialized*/)
{
    auto component_guard = Coordination::setCurrentComponent("ClusterMetadataDDLWorker::scheduleTasks");
    auto zookeeper = getZooKeeper();
    Coordination::Stat stat;
    zookeeper->getChildrenWatch(
        queue_dir,
        &stat,
        Coordination::WatchCallbackPtrOrEventPtr{queue_updated_event, ProfileEvents::ZooKeeperWatchTriggeredOther});
    zookeeper->get(max_log_ptr_path, nullptr, queue_updated_event);

    const UInt32 log_ptr = getLogPointer();
    const UInt32 max_log_ptr = getMaxLogPointer();
    const UInt32 logs_to_keep = readUInt32Node(zookeeper, logs_to_keep_path);
    if (reloadSnapshotAndAdvanceIfTooFarBehind(log_ptr, max_log_ptr, logs_to_keep))
        return;

    if (log_ptr < max_log_ptr)
    {
        if (!processCommittedEntriesOnce())
        {
            const String committed_path = joinPath(joinPath(queue_dir, DDLTaskBase::getLogEntryName(log_ptr + 1)), "committed");
            zookeeper->exists(committed_path, nullptr, queue_updated_event);
        }
    }
    else
        LOG_DEBUG(log, "No committed cluster metadata DDL entries to process");
}

bool ClusterMetadataDDLWorker::initializeMainThread()
{
    auto component_guard = Coordination::setCurrentComponent("ClusterMetadataDDLWorker::initializeMainThread");
    getAndSetZooKeeper();
    initKeeperLayout();
    return DDLWorker::initializeMainThread();
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
    /// holding `counter_lock` (see `enqueueMutation`), so no separate sequential `counter` subtree is
    /// needed and there is no allocate-then-commit window that could leak a gap.
    zookeeper->createIfNotExists(max_log_ptr_path, "0");
}

void ClusterMetadataDDLWorker::registerReplica()
{
    auto zookeeper = getZooKeeper();
    zookeeper->createIfNotExists(replicas_root, "");
    zookeeper->createIfNotExists(replica_path, node_name);
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
    setLogPointer(max_log_ptr);
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
    }
}

void ClusterMetadataDDLWorker::markReplicasActive(bool /*reinitialized*/)
{
    auto component_guard = Coordination::setCurrentComponent("ClusterMetadataDDLWorker::markReplicasActive");
    if (active_node_holder && active_node_holder_zookeeper && !active_node_holder_zookeeper->expired())
        return;

    auto zookeeper = getZooKeeper();
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

bool ClusterMetadataDDLWorker::processEntry(UInt32 entry_number)
{
    auto component_guard = Coordination::setCurrentComponent("ClusterMetadataDDLWorker::processEntry");
    auto zookeeper = getZooKeeper();
    const String entry_name = DDLTaskBase::getLogEntryName(entry_number);
    const String entry_path = joinPath(log_root, entry_name);
    const String committed_path = joinPath(entry_path, "committed");
    if (!zookeeper->exists(committed_path))
        return false;

    const String finished_path = joinPath(joinPath(entry_path, "finished"), node_name);
    if (zookeeper->exists(finished_path))
    {
        const auto status = ExecutionStatus::fromText(zookeeper->get(finished_path));
        if (status.code != 0)
            return false;
        setLogPointer(entry_number);
        return true;
    }
    const String entry_data = zookeeper->get(joinPath(entry_path, "entry"));
    const auto mutation = ClusterMetadataMutation::deserialize(storage->decodePayloadFromKeeper(entry_data));

    zookeeper->createIfNotExists(joinPath(entry_path, "active"), "");
    zookeeper->createIfNotExists(joinPath(entry_path, "finished"), "");

    const String entry_active_path = joinPath(joinPath(entry_path, "active"), node_name);
    zookeeper->deleteEphemeralNodeIfContentMatches(entry_active_path, node_name);
    zookeeper->create(entry_active_path, node_name, zkutil::CreateMode::Ephemeral);
    auto entry_active_holder = zkutil::EphemeralNodeHolder::existing(entry_active_path, *zookeeper);
    SCOPE_EXIT({
        zookeeper->tryRemove(entry_active_path);
        entry_active_holder->setAlreadyRemoved();
    });

    try
    {
        String digest;
        if (mutation_applier)
            digest = mutation_applier(mutation);
        else if (snapshot_reloader)
            digest = snapshot_reloader();
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cluster metadata DDL worker cannot apply entry without mutation applier or snapshot reloader");
        updateReplicaDigest(digest);

        const String status = ExecutionStatus(0).serializeText();
        zookeeper->createIfNotExists(finished_path, status);
        setLogPointer(entry_number);
        LOG_DEBUG(log, "Applied cluster metadata DDL log entry {}", entry_name);
        return true;
    }
    catch (...)
    {
        /// The committed metadata is already persisted in Keeper (it is written atomically with the
        /// log entry at enqueue time); the applier above only refreshes the in-memory snapshot. So an
        /// incremental apply failure can be recovered by reloading the full snapshot from Keeper,
        /// which is the source of truth. This prevents a single failing entry from permanently wedging
        /// the strictly-sequential consumer.
        tryLogCurrentException(
            log,
            fmt::format("Incremental apply of cluster metadata DDL log entry {} failed; attempting full snapshot reload from Keeper", entry_name));

        if (snapshot_reloader)
        {
            try
            {
                const String digest = snapshot_reloader();
                updateReplicaDigest(digest);
                zookeeper->createIfNotExists(finished_path, ExecutionStatus(0).serializeText());
                setLogPointer(entry_number);
                LOG_WARNING(log, "Recovered cluster metadata DDL log entry {} via full snapshot reload", entry_name);
                return true;
            }
            catch (...)
            {
                tryLogCurrentException(
                    log, fmt::format("Full snapshot reload while recovering cluster metadata DDL log entry {} also failed", entry_name));
            }
        }

        const String status = ExecutionStatus::fromCurrentException().serializeText();
        zookeeper->createOrUpdate(finished_path, status, zkutil::CreateMode::Persistent);
        return false;
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

void ClusterMetadataDDLWorker::setLogPointer(UInt32 log_pointer)
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
