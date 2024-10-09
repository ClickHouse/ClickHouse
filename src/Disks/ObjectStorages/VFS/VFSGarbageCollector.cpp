#include "VFSGarbageCollector.h"
#include <IO/ReadBufferFromEmptyFile.h>
#include <IO/ReadHelpers.h>
#include <IO/S3Common.h>
#include <Interpreters/Context.h>

#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/ZooKeeper/ZooKeeperLock.h>


// TODO: Add VFS events
// namespace ProfileEvents
// {
// extern const Event VFSGcRunsCompleted;
// extern const Event VFSGcRunsException;
// extern const Event VFSGcRunsSkipped;
// extern const Event VFSGcTotalSeconds;
// extern const Event VFSGcCumulativeWALSItemsMerged;
// }

namespace DB
{
using ms = std::chrono::milliseconds;

namespace FailPoints
{
extern const char vfs_gc_optimistic_lock_delay[];
}

VFSGarbageCollector::VFSGarbageCollector(
    const String & gc_name_,
    ObjectStoragePtr object_storage_,
    VFSLogPtr wal_,
    BackgroundSchedulePool & pool,
    const GarbageCollectorSettings & settings_)
    : gc_name(gc_name_)
    , object_storage(object_storage_)
    , vfs_shapshot_data(object_storage, gc_name)
    , wal(wal_)
    , settings(settings_)
    , log(getLogger(fmt::format("VFSGC({})", gc_name)))
{
    LOG_INFO(log, "GC started");
    initGCState();
    task_handle = pool.createTask(
        log->name(),
        [this]
        {
            try
            {
                run();
            }
            catch (...)
            {
                //ProfileEvents::increment(ProfileEvents::VFSGcRunsException);
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
            }
            task_handle->scheduleAfter(settings.gc_sleep_ms);
        });
    task_handle->activateAndSchedule();
}

void VFSGarbageCollector::shutdown()
{
    task_handle->deactivate();
}

void VFSGarbageCollector::initGCState() const
{
    auto zookeeper = getZookeeper();
    auto path = settings.zk_gc_path + "/garbage_collector";
    zookeeper->tryCreate(path, "", zkutil::CreateMode::Persistent);
    zookeeper->createIfNotExists(getZKSnapshotPath(), "");
}

VFSGarbageCollector::~VFSGarbageCollector()
{
    shutdown();
}


void VFSGarbageCollector::run()
{
    Stopwatch stop_watch;

    /// Optimization to reduce probability of clashes between replicas
    /// TODO alexfvk: templatize ZooKeeperLock to support ZooKeeperWithFaultInjection
    auto lock = zkutil::createSimpleZooKeeperLock(
        getZookeeper()->getKeeper(), settings.zk_gc_path + "/garbage_collector", "lock", fmt::format("garbare_collector_run"));
    if (!lock->tryLock())
    {
        LOG_DEBUG(log, "Skipped run due to pessimistic lock already acquired");
        return;
    }
    LOG_DEBUG(log, "GC acquired lock");

    updateSnapshot();

    //ProfileEvents::increment(ProfileEvents::VFSGcRunsCompleted);
    //ProfileEvents::increment(ProfileEvents::VFSGcTotalSeconds, stop_watch.elapsedMilliseconds() / 1000);
    LOG_DEBUG(log, "GC iteration finished");
}

String VFSGarbageCollector::getZKSnapshotPath() const
{
    return settings.zk_gc_path + "/garbage_collector/shapshot_meta";
}


SnapshotMetadata VFSGarbageCollector::getSnapshotMetadata() const
{
    auto zk_shapshot_metadata_path = getZKSnapshotPath();
    auto zookeeper = getZookeeper();

    Coordination::Stat stat;
    Coordination::Error error;
    String content;

    zookeeper->tryGet(zk_shapshot_metadata_path, content, &stat, nullptr, &error);

    if (error == Coordination::Error::ZOK)
        return SnapshotMetadata::deserialize(content, stat.version);

    throw Coordination::Exception(error);
}

ZooKeeperWithFaultInjectionPtr VFSGarbageCollector::getZookeeper() const
{
    return ZooKeeperWithFaultInjection::createInstance(
        settings.keeper_fault_injection_probability,
        settings.keeper_fault_injection_seed,
        Context::getGlobalContextInstance()->getZooKeeper(),
        fmt::format("VFSGarbageCollector({})", gc_name),
        log);
}

void VFSGarbageCollector::updateShapshotMetadata(const SnapshotMetadata & new_snapshot, int32_t znode_required_version) const
{
    auto zk_shapshot_metadata_path = getZKSnapshotPath();
    auto zookeeper = getZookeeper();
    /// For consistency
    zookeeper->set(zk_shapshot_metadata_path, new_snapshot.serialize(), znode_required_version);
}

void VFSGarbageCollector::updateSnapshot()
{
    LOG_TEST(log, "List object storage started ==============================");
    RelativePathsWithMetadata files;
    object_storage->listObjects("", files, 0);
    for (auto && file : files)
    {
        LOG_DEBUG(log, "Object: {} {}", file->relative_path, file->metadata->size_bytes);
    }
    LOG_TEST(log, "List object storage finished {} ==============================", files.size());

    SnapshotMetadata snapshot_meta = getSnapshotMetadata();
    auto wal_items_batch = wal->read(settings.batch_size);
    if (wal_items_batch.size() == 0ul)
    {
        LOG_DEBUG(log, "Merge snapshot exit due to empty wal.");
        return;
    }
    auto next_index = wal_items_batch.back().wal.index + 1;
    LOG_DEBUG(log, "Merge snapshot with {} entries from wal.", wal_items_batch.size());

    auto new_snaphot_meta = vfs_shapshot_data.mergeWithWals(std::move(wal_items_batch), snapshot_meta);

    LOG_TEST(log, "List object storage started ==============================");
    files.clear();
    object_storage->listObjects("", files, 0);
    for (auto && file : files)
    {
        LOG_TEST(log, "Object: {} {}", file->relative_path, file->metadata->size_bytes);
    }
    LOG_TEST(
        log,
        "List object storage finished {} remove snapshot path: {} ==============================",
        files.size(),
        snapshot_meta.object_storage_key);

    updateShapshotMetadata(new_snaphot_meta, snapshot_meta.znode_version);
    if (!snapshot_meta.is_initial_snaphot)
        object_storage->removeObject(StoredObject(snapshot_meta.object_storage_key));
    wal->dropUpTo(next_index);

    LOG_DEBUG(log, "Snapshot update finished with new shapshot key {}", new_snaphot_meta.object_storage_key);
}
}
