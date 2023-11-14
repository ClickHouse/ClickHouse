#include "ObjectStorageVFSGCThread.h"
#include <ranges>
#include "Common/ZooKeeper/ZooKeeperLock.h"
#include "Disks/ObjectStorages/DiskObjectStorageVFS.h"
#include "Interpreters/Context.h"

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

void ObjectStorageVFSGCThread::run()
{
    if (!zookeeper_lock->tryLock())
    {
        LOG_DEBUG(log, "Failed to acquire GC lock, sleeping");
        task->scheduleAfter(sleep_ms);
        return;
    }

    LOG_DEBUG(log, "Acquired GC lock");
    zkutil::ZooKeeper & zookeeper = *storage.zookeeper;

    VFSSnapshot snapshot;

    const auto [start_str, end_str] = std::ranges::minmax(zookeeper.getChildren(VFS_LOG_BASE_NODE));
    const size_t start_logpointer = parseFromString<size_t>(start_str);
    const size_t end_logpointer = parseFromString<size_t>(end_str);

    if (start_logpointer > 0)
        snapshot = loadSnapshot(start_logpointer - 1);

    ObsoleteObjects obsolete_objects = populateSnapshotWithLogEntries(snapshot, start_logpointer, end_logpointer);
    writeSnapshot(std::move(snapshot), end_logpointer);

    // TODO myrrc we should remove previous snapshot from object storage after writing current one.
    // smth like obsolete_objects.emplace_back(last_snapshot_name);

    removeObjectsFromObjectStorage(std::move(obsolete_objects));
    removeLogEntries(start_logpointer, end_logpointer);

    zookeeper_lock->unlock();
    task->scheduleAfter(sleep_ms);
}

VFSSnapshot ObjectStorageVFSGCThread::loadSnapshot(size_t end_logpointer)
{
    // TODO myrrc this file metadata is surely not present on local filesystem
    const String filename = fmt::format("vfs_snapshot_{}", end_logpointer);
    auto buf = storage.readFile(filename, {}, std::nullopt, std::nullopt);
    String snapshot_str;
    readStringUntilEOF(snapshot_str, *buf);
    return VFSSnapshot{}.deserialize(snapshot_str);
}

VFSSnapshot::ObsoleteObjects
ObjectStorageVFSGCThread::populateSnapshotWithLogEntries(VFSSnapshot & snapshot, size_t start_logpointer, size_t end_logpointer)
{
    VFSSnapshot::ObsoleteObjects out;

    Coordination::Requests ops;
    for (size_t i = start_logpointer; i <= end_logpointer; ++i)
        ops.emplace_back(zkutil::makeGetRequest(fmt::format("{}{}", VFS_LOG_ITEM, i)));

    std::vector<VFSTransactionLogItem> logs;

    for (const auto & item : storage.zookeeper->multi(ops))
        logs.emplace_back(VFSTransactionLogItem{}.deserialize(dynamic_cast<const Coordination::GetResponse &>(*item).data));

    return snapshot.update(logs);
}

void ObjectStorageVFSGCThread::writeSnapshot(VFSSnapshot && snapshot, size_t end_logpointer)
{
    const String filename = fmt::format("vfs_snapshot_{}", end_logpointer);
    auto buf = storage.writeFile(filename, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, {});
    writeString(snapshot.serialize(), *buf);
    buf->finalize();
}

void ObjectStorageVFSGCThread::removeObjectsFromObjectStorage(VFSSnapshot::ObsoleteObjects && items)
{
    LOG_INFO(log, "Removing objects {} from storage", fmt::join(items, ", "));
    // TODO myrrc remove file by having only remote path (without having metadata)
    //for (const auto& item : items)
    //storage.removeFile();
}

void ObjectStorageVFSGCThread::removeLogEntries(size_t start_logpointer, size_t end_logpointer)
{
    Coordination::Requests ops;
    for (size_t i = start_logpointer; i <= end_logpointer; ++i)
        ops.emplace_back(zkutil::makeRemoveRequest(fmt::format("{}{}", VFS_LOG_ITEM, i), -1));

    storage.zookeeper->multi(ops);
}
}
