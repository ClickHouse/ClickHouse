#include <Storages/MergeTree/ReplicatedMergeTreeAttachThread.h>
#include <Storages/StorageReplicatedMergeTree.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
}

ReplicatedMergeTreeAttachThread::ReplicatedMergeTreeAttachThread(StorageReplicatedMergeTree & storage_)
    : storage(storage_)
    , log_name(storage.getStorageID().getFullTableName() + " (ReplicatedMergeTreeAttachThread)")
    , log(&Poco::Logger::get(log_name))
{
    task = storage.getContext()->getSchedulePool().createTask(log_name, [this] { run(); });
}

void ReplicatedMergeTreeAttachThread::shutdown()
{
    need_shutdown = true;
    task->deactivate();
}

void ReplicatedMergeTreeAttachThread::run()
{
    try
    {
        LOG_INFO(log, "Table will be in readonly mode until initialization is finished");
        zookeeper = storage.current_zookeeper;
        if (!zookeeper)
            tryReconnect();

        const auto & zookeeper_path = storage.zookeeper_path;
        bool metadata_exists = withRetries([&] { return zookeeper->exists(zookeeper_path + "/metadata"); });
        if (!metadata_exists)
        {
            LOG_WARNING(log, "No metadata in ZooKeeper for {}: table will stay in readonly mode.", zookeeper_path);
            storage.has_metadata_in_zookeeper = false;
            notifyIfFirstTry();
            return;
        }

        storage.has_metadata_in_zookeeper = true;

        auto metadata_snapshot = storage.getInMemoryMetadataPtr();

        const auto & replica_path = storage.replica_path;
        /// May it be ZK lost not the whole root, so the upper check passed, but only the /replicas/replica
        /// folder.
        bool replica_path_exists = withRetries([&] { return zookeeper->exists(replica_path); });
        if (!replica_path_exists)
        {
            LOG_WARNING(log, "No metadata in ZooKeeper for {}: table will stay in readonly mode", replica_path);
            storage.has_metadata_in_zookeeper = false;
            notifyIfFirstTry();
            return;
        }

        /// In old tables this node may missing or be empty
        String replica_metadata;
        const bool replica_metadata_exists = withRetries([&] { return zookeeper->tryGet(replica_path + "/metadata", replica_metadata); });

        if (!replica_metadata_exists || replica_metadata.empty())
        {
            /// We have to check shared node granularity before we create ours.
            storage.other_replicas_fixed_granularity = withRetries([&] { return storage.checkFixedGranularityInZookeeper(zookeeper); });

            ReplicatedMergeTreeTableMetadata current_metadata(storage, metadata_snapshot);

            withRetries(
                [&]
                { zookeeper->createOrUpdate(replica_path + "/metadata", current_metadata.toString(), zkutil::CreateMode::Persistent); });
        }

        withRetries([&] { storage.checkTableStructure(zookeeper, replica_path, metadata_snapshot); });
        withRetries([&] { storage.checkParts(zookeeper, skip_sanity_checks); });

        withRetries(
            [&]
            {
                if (zookeeper->exists(replica_path + "/metadata_version"))
                {
                    storage.metadata_version = parse<int>(zookeeper->get(replica_path + "/metadata_version"));
                }
                else
                {
                    /// This replica was created with old clickhouse version, so we have
                    /// to take version of global node. If somebody will alter our
                    /// table, then we will fill /metadata_version node in zookeeper.
                    /// Otherwise on the next restart we can again use version from
                    /// shared metadata node because it was not changed.
                    Coordination::Stat metadata_stat;
                    zookeeper->get(zookeeper_path + "/metadata", &metadata_stat);
                    storage.metadata_version = metadata_stat.version;
                }
            });

        /// Temporary directories contain uninitialized results of Merges or Fetches (after forced restart),
        /// don't allow to reinitialize them, delete each of them immediately.
        storage.clearOldTemporaryDirectories(0, {"tmp_", "delete_tmp_"});
        storage.clearOldWriteAheadLogs();
        if (storage.getSettings()->merge_tree_enable_clear_old_broken_detached)
            storage.clearOldBrokenPartsFromDetachedDirecory();

        withRetries([&] { storage.createNewZooKeeperNodes(zookeeper); });
        withRetries([&] { storage.syncPinnedPartUUIDs(zookeeper); });

        withRetries([&] { storage.createTableSharedID(zookeeper); });

        notifyIfFirstTry();

        LOG_INFO(log, "Table is initialized");
        using enum StorageReplicatedMergeTree::InitializationPhase;
        {
            std::lock_guard lock(storage.initialization_mutex);
            if (!storage.startup_called)
            {
                storage.init_phase = INITIALIZATION_DONE;
                return;
            }

            storage.init_phase = STARTUP_IN_PROGRESS;
        }

        storage.startupImpl(std::move(zookeeper));
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::ABORTED && need_shutdown)
        {
            LOG_WARNING(log, "Shutdown called, cancelling initialization");
            return;
        }

        LOG_ERROR(log, "Initialization failed, table will remain readonly. Error: {}", e.message());
        notifyIfFirstTry();
    }
};

void ReplicatedMergeTreeAttachThread::notifyIfFirstTry()
{
    if (first_try)
    {
        first_try_done = true;
        first_try_done.notify_one();

        first_try = false;
    }
}

void ReplicatedMergeTreeAttachThread::tryReconnect()
{
    zookeeper = nullptr;
    while (!need_shutdown)
    {
        try
        {
            if (storage.zookeeper_name == storage.default_zookeeper_name)
                zookeeper = storage.getContext()->getZooKeeper();
            else
                zookeeper = storage.getContext()->getAuxiliaryZooKeeper(storage.zookeeper_name);
            return;
        }
        catch (...)
        {
            notifyIfFirstTry();
            LOG_WARNING(log, "Will try to reconnect to ZooKeeper in 10 seconds");
            std::this_thread::sleep_for(std::chrono::seconds(10));
        }
    }
}


void ReplicatedMergeTreeAttachThread::resetCurrentZooKeeper()
{
    storage.current_zookeeper = nullptr;
}

void ReplicatedMergeTreeAttachThread::setSkipSanityChecks(bool skip_sanity_checks_)
{
    skip_sanity_checks = skip_sanity_checks_;
}

}
