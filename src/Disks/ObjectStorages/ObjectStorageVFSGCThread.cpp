#include "ObjectStorageVFSGCThread.h"
#include <ranges>
#include "Common/ZooKeeper/ZooKeeperLock.h"
#include "Compression/CompressedReadBuffer.h"
#include "Compression/CompressedWriteBuffer.h"
#include "Disks/ObjectStorages/DiskObjectStorageVFS.h"
#include "Interpreters/Context.h"

namespace DB
{
using enum VFSTransactionLogItem::Type;

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int MEMORY_LIMIT_EXCEEDED;
}

static void checkIfCanLoadBatch()
{
    const Int64 limit = background_memory_tracker.getSoftLimit();
    const Int64 amount = background_memory_tracker.get();
    if (limit > 0 && amount >= limit)
        throw Exception(ErrorCodes::MEMORY_LIMIT_EXCEEDED, "Can't load log batch");
}

ObjectStorageVFSGCThread::ObjectStorageVFSGCThread(DiskObjectStorageVFS & storage_, ContextPtr context)
    : storage(storage_)
    , log(&Poco::Logger::get(fmt::format("VFSGC({})", storage_.getName())))
    , zookeeper_lock(storage.zookeeper, storage.traits.locks_node, "gc_lock")
{
    storage.zookeeper->createIfNotExists(storage.traits.last_snapshot_node, "");

    task = context->getSchedulePool().createTask(
        log->name(),
        [this]
        {
            try
            {
                run();
            }
            catch (...)
            {
                LOG_DEBUG(log, "Task threw an exception, rescheduling");
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
            }
        });

    task->activateAndSchedule();
}

ObjectStorageVFSGCThread::~ObjectStorageVFSGCThread() = default;

void ObjectStorageVFSGCThread::run()
{
    SCOPE_EXIT({
        zookeeper_lock.unlock();
        task->scheduleAfter(storage.gc_thread_sleep_ms);
    });

    if (!zookeeper_lock.tryLock())
    {
        LOG_DEBUG(log, "Failed to acquire lock, sleeping");
        return;
    }

    Strings batch = storage.zookeeper->getChildren(storage.traits.log_base_node);
    constexpr size_t batch_min_size = 1; // TODO myrrc should be a setting
    if (batch.size() < batch_min_size)
        return;
    const auto [start_str, end_str] = std::ranges::minmax(batch);
    const size_t start_logpointer = parseFromString<size_t>(start_str.substr(4)); // log- is a prefix
    const size_t end_logpointer = parseFromString<size_t>(end_str.substr(4));
    batch = {};

    LOG_DEBUG(log, "Acquired lock for log range [{};{}]", start_logpointer, end_logpointer);
    auto [snapshot, obsolete_objects] = getSnapshotWithLogEntries(start_logpointer, end_logpointer);

    const String snapshot_name = fmt::format("vfs_snapshot_{}", end_logpointer);
    const String snapshot_remote_path = writeSnapshot(std::move(snapshot), snapshot_name);

    LOG_TRACE(log, "Removing objects from storage: {}", fmt::join(obsolete_objects, "\n"));
    storage.object_storage->removeObjects(obsolete_objects);

    onBatchProcessed(start_logpointer, end_logpointer, snapshot_remote_path);

    LOG_DEBUG(log, "Removed lock for log range [{};{}]", start_logpointer, end_logpointer);
}

VFSSnapshotWithObsoleteObjects ObjectStorageVFSGCThread::getSnapshotWithLogEntries(size_t start_logpointer, size_t end_logpointer)
{
    const bool should_have_previous_snapshot = start_logpointer > 0;
    const size_t log_batch_length = end_logpointer - start_logpointer + 1;
    VFSSnapshotWithObsoleteObjects out;

    // TODO myrrc what if replica capturing log entry doesn't get correct snapshot due to stale read?
    // Should write snapshot logpointer here, read first child of log, and if lp + 1 != first_child issue
    // storage.zookeeper->sync(storage.traits.log_base_node)
    if (should_have_previous_snapshot)
    {
        auto last_snapshot = StoredObject{storage.zookeeper->get(storage.traits.last_snapshot_node)};
        auto snapshot_buf = storage.object_storage->readObject(last_snapshot);
        auto snapshot_compressed_buf = CompressedReadBuffer{*snapshot_buf};
        String snapshot_str;
        readStringUntilEOF(snapshot_str, snapshot_compressed_buf);
        out.snapshot = VFSSnapshot::deserialize(snapshot_str);
        out.obsolete_objects.emplace_back(std::move(last_snapshot));
        LOG_TRACE(log, "Loaded snapshot {}", out.snapshot);
    }

    checkIfCanLoadBatch();
    zkutil::ZooKeeper::MultiGetResponse responses;
    {
        Strings nodes(log_batch_length);
        for (size_t i = 0; i < log_batch_length; ++i)
            nodes[i] = getNode(start_logpointer + i);
        responses = storage.zookeeper->get(nodes);
    }

    for (size_t i = 0; i < log_batch_length; ++i)
    {
        auto entry = VFSTransactionLogItem::deserialize(std::move(responses[i].data));
        out.snapshot.update(entry, out.obsolete_objects);
        LOG_TRACE(log, "Log entry {}", entry);
    }

    return out;
}

String ObjectStorageVFSGCThread::writeSnapshot(VFSSnapshot && snapshot, const String & snapshot_name)
{
    LOG_DEBUG(log, "Writing snapshot {}", snapshot_name);
    LOG_TRACE(log, "{}", snapshot);

    const ObjectStorageKey key = storage.object_storage->generateObjectKeyForPath(snapshot_name);
    auto object = StoredObject{key.serialize()};

    auto buf = storage.object_storage->writeObject(object, WriteMode::Rewrite);
    auto compressed_buf = CompressedWriteBuffer{*buf};
    writeString(snapshot.serialize(), compressed_buf);
    compressed_buf.finalize();
    buf->finalize();

    return std::move(object.remote_path);
}

void ObjectStorageVFSGCThread::onBatchProcessed(size_t start_logpointer, size_t end_logpointer, const String & snapshot_remote_path)
{
    LOG_DEBUG(log, "Removing log range [{};{}], latest snapshot {}", start_logpointer, end_logpointer, snapshot_remote_path);

    const size_t log_batch_length = end_logpointer - start_logpointer + 1;
    Coordination::Requests requests(log_batch_length + 1);
    for (size_t i = 0; i < log_batch_length; ++i)
        requests[i] = zkutil::makeRemoveRequest(getNode(start_logpointer + i), -1);
    requests[log_batch_length] = zkutil::makeSetRequest(storage.traits.last_snapshot_node, snapshot_remote_path, -1);

    storage.zookeeper->multi(requests);
}

String ObjectStorageVFSGCThread::getNode(size_t id) const
{
    // Zookeeper's sequential node is 10 digits with padding zeros
    return fmt::format("{}{:010}", storage.traits.log_item, id);
}
}
