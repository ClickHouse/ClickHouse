#include "VFSGarbageCollector.h"
#include <IO/ReadBufferFromEmptyFile.h>
#include <IO/ReadHelpers.h>
#include <IO/S3Common.h>
#include <Interpreters/Context.h>

#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/ZooKeeper/ZooKeeperLock.h>


namespace ProfileEvents
{
extern const Event VFSGcRunsCompleted;
extern const Event VFSGcRunsException;
extern const Event VFSGcRunsSkipped;
extern const Event VFSGcTotalSeconds;
extern const Event VFSGcCumulativeWALSItemsMerged;
}

namespace DB
{
using ms = std::chrono::milliseconds;
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int INVALID_STATE;
}
namespace FailPoints
{
extern const char vfs_gc_optimistic_lock_delay[];
}

VFSGarbageCollector::VFSGarbageCollector(
    const String & gc_name_,
    ObjectStoragePtr object_storage_,
    VFSLog & wal_,
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
    *static_cast<BackgroundSchedulePoolTaskHolder *>(this) = pool.createTask(
        log->name(),
        [this]
        {
            try
            {
                run();
            }
            catch (...)
            {
                ProfileEvents::increment(ProfileEvents::VFSGcRunsException);
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
            }
            (*this)->scheduleAfter(settings.gc_sleep_ms);
        });
    (*this)->activateAndSchedule();
}

void VFSGarbageCollector::initGCState() const
{
    auto zookeeper = getZookeeper();
    auto path = settings.zk_gc_path + "/garbage_collector";
    zookeeper->tryCreate(path, "", zkutil::CreateMode::Persistent);
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

    ProfileEvents::increment(ProfileEvents::VFSGcRunsCompleted);
    ProfileEvents::increment(ProfileEvents::VFSGcTotalSeconds, stop_watch.elapsedMilliseconds() / 1000);
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
    SnapshotMetadata snapshot_meta = getSnapshotMetadata();
    auto wal_items_batch = wal.read(settings.batch_size);
    if (wal_items_batch.size() == 0ul)
    {
        LOG_DEBUG(log, "Merge snapshot exit due to empty wal.");
        return;
    }
    LOG_DEBUG(log, "Merge snapshot with {} entries from wal.", wal_items_batch.size());

    auto new_snaphot_meta = vfs_shapshot_data.mergeWithWals(std::move(wal_items_batch), snapshot_meta);

    updateShapshotMetadata(new_snaphot_meta, snapshot_meta.znode_version);
    wal.dropUpTo(wal_items_batch.back().wal.index + 1);

    LOG_DEBUG(log, "Snapshot update finished with new shapshot key {}", new_snaphot_meta.object_storage_key);
}
}
