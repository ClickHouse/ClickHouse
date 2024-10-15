#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/ReplicatedMergeTreeAttachThread.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/ZooKeeper/IKeeper.h>

namespace CurrentMetrics
{
    extern const Metric ReadonlyReplica;
}

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsSeconds initialization_retry_period;
}

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
    extern const int REPLICA_STATUS_CHANGED;
    extern const int LOGICAL_ERROR;
}

ReplicatedMergeTreeAttachThread::ReplicatedMergeTreeAttachThread(StorageReplicatedMergeTree & storage_)
    : storage(storage_)
    , log_name(storage.getStorageID().getFullTableName() + " (ReplicatedMergeTreeAttachThread)")
    , log(getLogger(log_name))
{
    task = storage.getContext()->getSchedulePool().createTask(log_name, [this] { run(); });
    const auto storage_settings = storage.getSettings();
    retry_period = (*storage_settings)[MergeTreeSetting::initialization_retry_period].totalSeconds();
}

ReplicatedMergeTreeAttachThread::~ReplicatedMergeTreeAttachThread()
{
    shutdown();
}

void ReplicatedMergeTreeAttachThread::shutdown()
{
    if (!shutdown_called.exchange(true))
    {
        task->deactivate();
        LOG_INFO(log, "Attach thread finished");
    }
}

void ReplicatedMergeTreeAttachThread::run()
{
    bool needs_retry{false};
    try
    {
        // we delay the first reconnect if the storage failed to connect to ZK initially
        if (!first_try_done && !storage.current_zookeeper)
        {
            needs_retry = true;
        }
        else
        {
            runImpl();
            finalizeInitialization();
        }
    }
    catch (const Exception & e)
    {
        if (const auto * coordination_exception = dynamic_cast<const Coordination::Exception *>(&e))
            needs_retry = Coordination::isHardwareError(coordination_exception->code);
        else if (e.code() == ErrorCodes::REPLICA_STATUS_CHANGED)
            needs_retry = true;

        if (needs_retry)
        {
            LOG_ERROR(log, "Initialization failed. Error: {}", getCurrentExceptionMessage(/* with_stacktrace */ true));
        }
        else
        {
            LOG_ERROR(log, "Initialization failed, table will remain readonly. Error: {}", getCurrentExceptionMessage(/* with_stacktrace */ true));
            storage.initialization_done = true;
        }

        if (!std::exchange(storage.is_readonly_metric_set, true))
            CurrentMetrics::add(CurrentMetrics::ReadonlyReplica);
    }

    if (!first_try_done.exchange(true))
        first_try_done.notify_one();

    if (shutdown_called)
    {
        if (std::exchange(storage.is_readonly_metric_set, false))
        {
            CurrentMetrics::sub(CurrentMetrics::ReadonlyReplica);
            chassert(CurrentMetrics::get(CurrentMetrics::ReadonlyReplica) >= 0);
        }

        LOG_WARNING(log, "Shutdown called, cancelling initialization");
        return;
    }

    if (needs_retry)
    {
        LOG_INFO(log, "Will retry initialization in {}s", retry_period);
        task->scheduleAfter(retry_period * 1000);
    }
}

void ReplicatedMergeTreeAttachThread::checkHasReplicaMetadataInZooKeeper(const zkutil::ZooKeeperPtr & zookeeper, const String & replica_path)
{
    /// Since 20.4 and until 22.9 "/metadata" node was created on replica startup and "/metadata_version" was created on ALTER.
    /// Since 21.12 we could use "/metadata" to check if replica is dropped (see StorageReplicatedMergeTree::dropReplica),
    /// but it did not work correctly, because "/metadata" node was re-created on server startup.
    /// Since 22.9 we do not recreate these nodes and use "/host" to check if replica is dropped.

    String replica_metadata;
    const bool replica_metadata_exists = zookeeper->tryGet(replica_path + "/metadata", replica_metadata);
    if (!replica_metadata_exists || replica_metadata.empty())
    {
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Upgrade from 20.3 and older to 22.9 and newer "
                        "should be done through an intermediate version (failed to get metadata or metadata_version for {},"
                        "assuming it's because of upgrading)", replica_path);
    }
}

Int32 ReplicatedMergeTreeAttachThread::fixReplicaMetadataVersionIfNeeded(zkutil::ZooKeeperPtr zookeeper)
{
    const String & zookeeper_path = storage.zookeeper_path;
    const String & replica_path = storage.replica_path;
    const bool replica_readonly = storage.is_readonly;

    for (size_t i = 0; i != 2; ++i)
    {
        String replica_metadata_version_str;
        const bool replica_metadata_version_exists = zookeeper->tryGet(replica_path + "/metadata_version", replica_metadata_version_str);
        if (!replica_metadata_version_exists)
            return -1;

        const Int32 metadata_version = parse<Int32>(replica_metadata_version_str);

        if (metadata_version != 0 || replica_readonly)
        {
            /// No need to fix anything
            return metadata_version;
        }

        Coordination::Stat stat;
        zookeeper->get(fs::path(zookeeper_path) / "metadata", &stat);
        if (stat.version == 0)
        {
            /// No need to fix anything
            return metadata_version;
        }

        ReplicatedMergeTreeQueue & queue = storage.queue;
        queue.pullLogsToQueue(zookeeper);
        if (queue.getStatus().metadata_alters_in_queue != 0)
        {
            LOG_DEBUG(log, "No need to update metadata_version as there are ALTER_METADATA entries in the queue");
            return metadata_version;
        }

        const Coordination::Requests ops = {
            zkutil::makeSetRequest(fs::path(replica_path) / "metadata_version", std::to_string(stat.version), 0),
            zkutil::makeCheckRequest(fs::path(zookeeper_path) / "metadata", stat.version),
        };
        Coordination::Responses ops_responses;
        const auto code = zookeeper->tryMulti(ops, ops_responses);
        if (code == Coordination::Error::ZOK)
        {
            LOG_DEBUG(log, "Successfully set metadata_version to {}", stat.version);
            return stat.version;
        }
        if (code != Coordination::Error::ZBADVERSION)
        {
            throw zkutil::KeeperException(code);
        }
    }

    /// Second attempt is only possible if metadata_version != 0 or metadata.version changed during the first attempt.
    /// If metadata_version != 0, on second attempt we will return the new metadata_version.
    /// If metadata.version changed, on second attempt we will either get metadata_version != 0 and return the new metadata_version or we will get metadata_alters_in_queue != 0 and return 0.
    /// Either way, on second attempt this method should return.
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to fix replica metadata_version in ZooKeeper after two attempts");
}

void ReplicatedMergeTreeAttachThread::runImpl()
{
    storage.setZooKeeper();

    auto zookeeper = storage.getZooKeeper();
    const auto & zookeeper_path = storage.zookeeper_path;
    bool metadata_exists = zookeeper->exists(zookeeper_path + "/metadata");
    if (!metadata_exists)
    {
        LOG_WARNING(log, "No metadata in ZooKeeper for {}: table will stay in readonly mode.", zookeeper_path);
        storage.has_metadata_in_zookeeper = false;
        return;
    }

    auto metadata_snapshot = storage.getInMemoryMetadataPtr();

    const auto & replica_path = storage.replica_path;
    /// May it be ZK lost not the whole root, so the upper check passed, but only the /replicas/replica
    /// folder.
    bool replica_path_exists = zookeeper->exists(replica_path);
    if (!replica_path_exists)
    {
        LOG_WARNING(log, "No metadata in ZooKeeper for {}: table will stay in readonly mode", replica_path);
        storage.has_metadata_in_zookeeper = false;
        return;
    }

    bool host_node_exists = zookeeper->exists(replica_path + "/host");
    if (!host_node_exists)
    {
        LOG_WARNING(log, "Replica {} is dropped (but metadata is not completely removed from ZooKeeper), "
                         "table will stay in readonly mode", replica_path);
        storage.has_metadata_in_zookeeper = false;
        return;
    }

    storage.has_metadata_in_zookeeper = true;

    checkHasReplicaMetadataInZooKeeper(zookeeper, replica_path);

    /// Just in case it was not removed earlier due to connection loss
    zookeeper->tryRemove(replica_path + "/flags/force_restore_data");

    const Int32 replica_metadata_version = fixReplicaMetadataVersionIfNeeded(zookeeper);
    const bool replica_metadata_version_exists = replica_metadata_version != -1;
    if (replica_metadata_version_exists)
    {
        storage.setInMemoryMetadata(metadata_snapshot->withMetadataVersion(replica_metadata_version));
    }
    else
    {
        /// Table was created before 20.4 and was never altered,
        /// let's initialize replica metadata version from global metadata version.
        Coordination::Stat table_metadata_version_stat;
        zookeeper->get(zookeeper_path + "/metadata", &table_metadata_version_stat);

        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeCheckRequest(zookeeper_path + "/metadata", table_metadata_version_stat.version));
        ops.emplace_back(zkutil::makeCreateRequest(replica_path + "/metadata_version", toString(table_metadata_version_stat.version), zkutil::CreateMode::Persistent));

        Coordination::Responses res;
        auto code = zookeeper->tryMulti(ops, res);

        if (code == Coordination::Error::ZBADVERSION)
            throw Exception(ErrorCodes::REPLICA_STATUS_CHANGED, "Failed to initialize metadata_version "
                                                                "because table was concurrently altered, will retry");

        zkutil::KeeperMultiException::check(code, ops, res);
    }

    storage.checkTableStructure(replica_path, metadata_snapshot);
    storage.checkParts(skip_sanity_checks);

    /// Temporary directories contain uninitialized results of Merges or Fetches (after forced restart),
    /// don't allow to reinitialize them, delete each of them immediately.
    storage.clearOldTemporaryDirectories(0, {"tmp_", "delete_tmp_", "tmp-fetch_"});

    storage.createNewZooKeeperNodes();
    storage.syncPinnedPartUUIDs();

    std::lock_guard lock(storage.table_shared_id_mutex);
    storage.createTableSharedID();
};

void ReplicatedMergeTreeAttachThread::finalizeInitialization() TSA_NO_THREAD_SAFETY_ANALYSIS
{
    storage.startupImpl(/* from_attach_thread */ true);
    storage.initialization_done = true;
    LOG_INFO(log, "Table is initialized");
}

void ReplicatedMergeTreeAttachThread::setSkipSanityChecks(bool skip_sanity_checks_)
{
    skip_sanity_checks = skip_sanity_checks_;
}

}
