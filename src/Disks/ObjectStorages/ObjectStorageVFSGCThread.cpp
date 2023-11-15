#include "ObjectStorageVFSGCThread.h"
#include "Common/ZooKeeper/ZooKeeperLock.h"
#include "Disks/ObjectStorages/DiskObjectStorageVFS.h"
#include "Interpreters/Context.h"

static constexpr auto VFS_SNAPSHOT_PREFIX = "vfs_snapshot_";

String getNode(size_t id)
{
    // Zookeeper's sequential node is 10 digits with padding zeros
    return fmt::format("{}{:010}", DB::VFS_LOG_ITEM, id);
}

namespace DB
{
ObjectStorageVFSGCThread::ObjectStorageVFSGCThread(DiskObjectStorageVFS & storage_, ContextPtr context)
    : storage(storage_)
    , log_name("ObjectStorageVFS (GC thread)")
    , log(&Poco::Logger::get(log_name))
    , zookeeper_lock(zkutil::createSimpleZooKeeperLock(storage.zookeeper, VFS_BASE_NODE, "lock", ""))
    , sleep_ms(10'000) // TODO myrrc should pick this from settings
{
    task = context->getSchedulePool().createTask(log_name, [this] { run(); });
}

ObjectStorageVFSGCThread::~ObjectStorageVFSGCThread() = default;

// TODO myrrc handle exceptions -- reschedule task if interrupted
void ObjectStorageVFSGCThread::run()
{
    if (!zookeeper_lock->tryLock())
    {
        LOG_DEBUG(log, "Failed to acquire lock, sleeping");
        task->scheduleAfter(sleep_ms);
        return;
    }

    const auto [start_str, end_str] = std::ranges::minmax(storage.zookeeper->getChildren(VFS_LOG_BASE_NODE));
    // log- is a prefix
    const size_t start_logpointer = start_str.empty() ? 0 : parseFromString<size_t>(start_str.substr(4));
    const size_t end_logpointer = end_str.empty() ? 0 : parseFromString<size_t>(end_str.substr(4));

    LOG_DEBUG(log, "Acquired lock for log range [{};{}]", start_logpointer, end_logpointer);

    if (end_logpointer > 0) [[likely]]
    {
        auto [snapshot, obsolete_objects] = getSnapshotWithLogEntries(start_logpointer, end_logpointer);

        const String snapshot_name = fmt::format("{}{}", VFS_SNAPSHOT_PREFIX, end_logpointer);
        writeSnapshot(std::move(snapshot), snapshot_name);

        removeObjectsFromObjectStorage(obsolete_objects);
        removeLogEntries(start_logpointer, end_logpointer);
    }

    zookeeper_lock->unlock();
    task->scheduleAfter(sleep_ms);
}

VFSSnapshotWithObsoleteObjects ObjectStorageVFSGCThread::getSnapshotWithLogEntries(size_t start_logpointer, size_t end_logpointer)
{
    if (start_logpointer == 0)
        return {};

    /// Precondition: when we write a snapshot on a previous replica, the snapshot writing operation is
    /// put in log, so when we process next batch, we can get the snapshot remote path from log. Then we
    /// construct a StoredObject and read directly from it.

    VFSSnapshotWithObsoleteObjects out;

    Coordination::Requests ops;
    for (size_t i = start_logpointer; i <= end_logpointer; ++i)
        ops.emplace_back(zkutil::makeGetRequest(getNode(i)));

    std::vector<VFSTransactionLogItem> logs;
    std::optional<String> previous_snapshot_remote_path;

    for (const auto & item : storage.zookeeper->multi(ops))
    {
        auto log_item = VFSTransactionLogItem{}.deserialize(dynamic_cast<const Coordination::GetResponse &>(*item).data);

        if (log_item.type == VFSTransactionLogItem::Type::CreateInode //NOLINT
            && log_item.local_path.starts_with(VFS_SNAPSHOT_PREFIX))
            previous_snapshot_remote_path.emplace(log_item.remote_path);

        logs.emplace_back(std::move(log_item));
    }

    const StoredObject previous_snapshot{*previous_snapshot_remote_path};
    auto snapshot_buf = storage.readObject(previous_snapshot);
    String snapshot_str;
    readStringUntilEOF(snapshot_str, *snapshot_buf);

    out.snapshot = VFSSnapshot{}.deserialize(snapshot_str);
    out.obsolete_objects = out.snapshot.update(logs);
    out.obsolete_objects.emplace_back(previous_snapshot);

    return out;
}

void ObjectStorageVFSGCThread::writeSnapshot(VFSSnapshot && snapshot, const String & snapshot_name)
{
    LOG_DEBUG(log, "Writing snapshot {}", snapshot_name);

    auto buf = storage.writeFile(snapshot_name, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, {});
    writeString(snapshot.serialize(), *buf);
    buf->finalize();
}

void ObjectStorageVFSGCThread::removeObjectsFromObjectStorage(const VFSSnapshot::ObsoleteObjects & objects)
{
    LOG_DEBUG(log, "Removing objects {} from storage", fmt::join(objects, ", "));
    storage.removeObjects(objects);
}

void ObjectStorageVFSGCThread::removeLogEntries(size_t start_logpointer, size_t end_logpointer)
{
    LOG_DEBUG(log, "Removing log range [{};{}]", start_logpointer, end_logpointer);
    Coordination::Requests ops;
    for (size_t i = start_logpointer; i <= end_logpointer; ++i)
        ops.emplace_back(zkutil::makeRemoveRequest(getNode(i), -1));

    storage.zookeeper->multi(ops);
}
}
