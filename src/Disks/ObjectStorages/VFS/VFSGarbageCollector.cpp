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
    WAL::AppendLog & alog_,
    BackgroundSchedulePool & pool,
    const GarbageCollectorSettings & settings_)
    : gc_name(gc_name_)
    , object_storage(object_storage_)
    , alog(alog_)
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


void VFSGarbageCollector::run() const
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

std::unique_ptr<ReadBuffer> VFSGarbageCollector::getShapshotReadBuffer(const SnapshotMetadata & snapshot_meta) const
{
    if (!snapshot_meta.is_initial_snaphot)
    {
        StoredObject object(snapshot_meta.object_storage_key, "", snapshot_meta.total_size);
        // to do read settings.
        auto res = object_storage->readObject(object, {});
        return res;
    }
    return std::make_unique<ReadBufferFromEmptyFile>();
}

std::pair<std::unique_ptr<WriteBuffer>, StoredObject>
VFSGarbageCollector::getShapshotWriteBufferAndSnaphotObject(const SnapshotMetadata & snapshot_meta) const
{
    String new_object_path = fmt::format("/vfs_shapshots/shapshot_{}", snapshot_meta.znode_version + 1);
    auto new_object_key = object_storage->generateObjectKeyForPath(new_object_path);
    StoredObject new_object(new_object_key.serialize());
    std::unique_ptr<WriteBuffer> new_shapshot_write_buffer = object_storage->writeObject(new_object, WriteMode::Rewrite);

    return {std::move(new_shapshot_write_buffer), new_object};
}


void VFSGarbageCollector::removeShapshotEntires(const VFSSnapshotEntries & entires_to_remove) const
{
    StoredObjects objects_to_remove;
    objects_to_remove.reserve(entires_to_remove.size());

    for (const auto & entry : entires_to_remove)
    {
        objects_to_remove.emplace_back(entry.remote_path);
    }
    object_storage->removeObjects(objects_to_remove);
}


void VFSGarbageCollector::updateShapshotMetadata(const SnapshotMetadata & new_snapshot, int32_t znode_required_version) const
{
    auto zk_shapshot_metadata_path = getZKSnapshotPath();
    auto zookeeper = getZookeeper();
    /// For consistency
    zookeeper->set(zk_shapshot_metadata_path, new_snapshot.serialize(), znode_required_version);
}

void VFSGarbageCollector::updateSnapshot() const
{
    SnapshotMetadata snapshot_meta = getSnapshotMetadata();

    /// For most of object stroges (like s3 or azure) we don't need the object path, it's generated randomly.
    /// But other ones reqiested to set it manually.
    std::unique_ptr<ReadBuffer> shapshot_read_buffer = getShapshotReadBuffer(snapshot_meta);
    auto [new_shapshot_write_buffer, new_object] = getShapshotWriteBufferAndSnaphotObject(snapshot_meta);

    auto [wal_items_batch, max_entry_index] = getWalItems(alog, settings.batch_size);

    if (wal_items_batch.size() == 0ul)
    {
        LOG_DEBUG(log, "Merge snapshot exit due to empty wal.");
        return;
    }

    LOG_DEBUG(log, "Merge snapshot with {} entries from wal.", wal_items_batch.size());
    auto entires_to_remove = mergeWithWals(wal_items_batch, *shapshot_read_buffer, *new_shapshot_write_buffer);

    SnapshotMetadata new_snaphot_meta(new_object.remote_path);

    removeShapshotEntires(entires_to_remove);
    updateShapshotMetadata(new_snaphot_meta, snapshot_meta.znode_version);
    alog.dropUpTo(max_entry_index + 1);

    LOG_DEBUG(
        log,
        "Snapshot update finished with removing {} from object storage and new shapshot key {}",
        entires_to_remove.size(),
        new_object.remote_path);
}
}
