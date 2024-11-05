#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/ReplicatedMergeTreeAttachThread.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQueue.h>
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
