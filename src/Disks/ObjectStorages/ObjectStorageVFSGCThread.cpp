#include "ObjectStorageVFSGCThread.h"
#include <ranges>
#include "Common/ZooKeeper/ZooKeeperLock.h"
#include "Compression/CompressedReadBuffer.h"
#include "Compression/CompressedWriteBuffer.h"
#include "DiskObjectStorageVFS.h"
#include "IO/ReadBufferFromEmptyFile.h"
#include "Interpreters/Context.h"

namespace DB
{
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

    Strings log_items_batch = storage.zookeeper->getChildren(storage.traits.log_base_node);
    constexpr size_t batch_min_size = 1; // TODO myrrc should be a setting
    // TODO myrrc should (possibly?) be a setting. The batch size must not be too large as we put it in memory
    constexpr size_t batch_max_size = 5000;
    if (log_items_batch.size() < batch_min_size)
        return;

    // TODO myrrc ZK should return children in lexicographical order. If it were true, we could
    // get minmax by (begin(), rbegin()), but it's not the case so we have to traverse all range
    const auto [start_str, end_str] = std::ranges::minmax(log_items_batch);
    const size_t start_logpointer = parseFromString<size_t>(start_str.substr(4)); // log- is a prefix
    const size_t end_logpointer = std::min(parseFromString<size_t>(end_str.substr(4)), start_logpointer + batch_max_size);
    log_items_batch = {};

    LOG_DEBUG(log, "Acquired lock for log range [{};{}]", start_logpointer, end_logpointer);
    const PathAndVersion pair = updateSnapshotWithLogEntries(start_logpointer, end_logpointer);
    onBatchProcessed(start_logpointer, end_logpointer, pair);
    LOG_DEBUG(log, "Removed lock for log range [{};{}]", start_logpointer, end_logpointer);
}

ObjectStorageVFSGCThread::PathAndVersion
ObjectStorageVFSGCThread::updateSnapshotWithLogEntries(size_t start_logpointer, size_t end_logpointer)
{
    const bool should_have_previous_snapshot = start_logpointer > 0;

    Coordination::Stat old_snapshot_stat;
    const String old_snapshot_remote_path = storage.zookeeper->get(storage.traits.last_snapshot_node, &old_snapshot_stat);
    const StoredObject old_snapshot_object{old_snapshot_remote_path};

    // TODO myrrc what if replica capturing log entry doesn't get correct snapshot due to stale read?
    // Should write snapshot logpointer here, read first child of log, and if lp + 1 != first_child issue
    // storage.zookeeper->sync(storage.traits.log_base_node)
    auto old_snapshot_uncompressed_buf = should_have_previous_snapshot
        ? storage.object_storage->readObject(old_snapshot_object)
        : std::unique_ptr<ReadBufferFromFileBase>(std::make_unique<ReadBufferFromEmptyFile>());

    CompressedReadBuffer old_snapshot_buf{*old_snapshot_uncompressed_buf};

    // TODO myrrc use snapshot name instead of random name to locate objects on s3 without involving clickhouse
    const String new_snapshot_name = fmt::format("vfs_snapshot_{}", end_logpointer);
    const ObjectStorageKey new_snapshot_key = storage.object_storage->generateObjectKeyForPath(new_snapshot_name);
    StoredObject new_snapshot{new_snapshot_key.serialize()};

    auto new_snapshot_uncompressed_buf = storage.object_storage->writeObject(new_snapshot, WriteMode::Rewrite);
    CompressedWriteBuffer new_snapshot_buf{*new_snapshot_uncompressed_buf};

    StoredObjects obsolete = getBatch(start_logpointer, end_logpointer).mergeWithSnapshot(old_snapshot_buf, new_snapshot_buf, log);
    if (should_have_previous_snapshot)
        obsolete.emplace_back(old_snapshot_object);

    new_snapshot_buf.finalize(); // We need to write new snapshot to s3 before removing old one
    new_snapshot_uncompressed_buf->finalize();

    storage.object_storage->removeObjects(obsolete);

    return {std::move(new_snapshot.remote_path), old_snapshot_stat.version};
}

VFSLogItem ObjectStorageVFSGCThread::getBatch(size_t start_logpointer, size_t end_logpointer) const
{
    const size_t log_batch_length = end_logpointer - start_logpointer + 1;

    Strings nodes(log_batch_length);
    for (size_t i = 0; i < log_batch_length; ++i)
        nodes[i] = getNode(start_logpointer + i);
    auto responses = storage.zookeeper->get(nodes);
    nodes = {};

    VFSLogItem out;
    for (size_t i = 0; i < log_batch_length; ++i)
    {
        auto item = VFSLogItem::parse(responses[i].data);
        LOG_TRACE(log, "Log item {}", item);
        out.merge(std::move(item));
    }
    LOG_TRACE(log, "Merged batch:\n{}", out);

    return out;
}

void ObjectStorageVFSGCThread::onBatchProcessed(size_t start_logpointer, size_t end_logpointer, const PathAndVersion & pair)
{
    LOG_DEBUG(log, "Removing log range [{};{}], latest snapshot {}", start_logpointer, end_logpointer, pair.remote_path);

    const size_t log_batch_length = end_logpointer - start_logpointer + 1;
    Coordination::Requests requests(log_batch_length + 1);
    for (size_t i = 0; i < log_batch_length; ++i)
        requests[i] = zkutil::makeRemoveRequest(getNode(start_logpointer + i), -1);
    requests[log_batch_length] = zkutil::makeSetRequest(storage.traits.last_snapshot_node, pair.remote_path, pair.version);

    storage.zookeeper->multi(requests);
}

String ObjectStorageVFSGCThread::getNode(size_t id) const
{
    // Zookeeper's sequential node is 10 digits with padding zeros
    return fmt::format("{}{:010}", storage.traits.log_item, id);
}
}
