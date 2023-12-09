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
}

static constexpr std::string_view VFS_SNAPSHOT_PREFIX = "vfs_snapshot_";

ObjectStorageVFSGCThread::ObjectStorageVFSGCThread(DiskObjectStorageVFS & storage_, ContextPtr context)
    : storage(storage_)
    , log(&Poco::Logger::get(fmt::format("VFSGC({})", storage_.getName())))
    , zookeeper_lock(storage.zookeeper, storage.traits.locks_node, "gc_lock")
{
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

    const Strings batch = storage.zookeeper->getChildren(storage.traits.log_base_node);
    constexpr size_t batch_min_size = 1; // TODO myrrc should be a setting
    if (batch.size() < batch_min_size)
        return;

    const auto [start_str, end_str] = std::ranges::minmax(batch);
    const size_t start_logpointer = parseFromString<size_t>(start_str.substr(4)); // log- is a prefix
    const size_t end_logpointer = parseFromString<size_t>(end_str.substr(4));

    LOG_DEBUG(log, "Acquired lock for log range [{};{}]", start_logpointer, end_logpointer);
    auto [snapshot, obsolete_objects] = getSnapshotWithLogEntries(start_logpointer, end_logpointer);

    const String snapshot_name = fmt::format("{}{}", VFS_SNAPSHOT_PREFIX, end_logpointer);
    writeSnapshot(std::move(snapshot), snapshot_name);

    LOG_TRACE(log, "Removing objects from storage: {}", fmt::join(obsolete_objects, "\n"));
    storage.object_storage->removeObjects(obsolete_objects);

    removeLogEntries(start_logpointer, end_logpointer);

    LOG_DEBUG(log, "Removed lock for log range [{};{}]", start_logpointer, end_logpointer);
}

VFSSnapshotWithObsoleteObjects ObjectStorageVFSGCThread::getSnapshotWithLogEntries(size_t start_logpointer, size_t end_logpointer)
{
    const bool has_previous_snapshot = start_logpointer > 0;
    const size_t log_batch_length = end_logpointer - start_logpointer + 1;
    VFSSnapshotWithObsoleteObjects out;

    Coordination::Requests requests(log_batch_length);
    for (size_t i = 0; i < log_batch_length; ++i)
        requests[i] = zkutil::makeGetRequest(getNode(start_logpointer + i));

    std::vector<VFSTransactionLogItem> log_batch(log_batch_length);
    VFSTransactionLogItem previous_snapshot_log_item;

    /// Precondition: when we write a snapshot on a previous replica, the snapshot writing operation is
    /// put in log, so when we process next batch, we can get the snapshot remote path from log. Then we
    /// construct a StoredObject and read directly from it.
    const Coordination::Responses responses = storage.zookeeper->multi(requests);

    for (size_t i = 0; i < log_batch_length; ++i)
    {
        const String & log_item_str = dynamic_cast<const Coordination::GetResponse &>(*responses[i]).data;
        auto log_item = VFSTransactionLogItem::deserialize(log_item_str);

        if (log_item.type == CreateInode && log_item.local_path.starts_with(VFS_SNAPSHOT_PREFIX))
        {
            if (!previous_snapshot_log_item.remote_path.empty())
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "More than one snapshot entry ({}, {}) found for log batch [{};{}]",
                    previous_snapshot_log_item,
                    log_item,
                    start_logpointer,
                    end_logpointer);

            previous_snapshot_log_item = log_item;
        }

        log_batch[i] = std::move(log_item);
    }

    if (has_previous_snapshot)
    {
        // TODO myrrc what if replica capturing log entry doesn't get snapshot due to stale read?
        // Consider using zk->sync(VFS_LOG_BASE_NODE)
        if (previous_snapshot_log_item.remote_path.empty())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "No snapshot for {} found in log entries [{};{}]",
                start_logpointer - 1,
                start_logpointer,
                end_logpointer);
        const size_t snapshot_logpointer
            = parseFromString<size_t>(previous_snapshot_log_item.local_path.substr(VFS_SNAPSHOT_PREFIX.size()));
        if (snapshot_logpointer + 1 != start_logpointer)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Snapshot logpointer {} != start logpointer {}", snapshot_logpointer, start_logpointer);

        previous_snapshot_log_item.type = Unlink;
        // Issue previous snapshot remote file for removal (no local metadata file as we use direct readObject)
        log_batch.emplace_back(previous_snapshot_log_item);

        auto snapshot_buf = storage.object_storage->readObject(previous_snapshot_log_item);
        auto snapshot_compressed_buf = CompressedReadBuffer{*snapshot_buf};
        String snapshot_str;
        readStringUntilEOF(snapshot_str, snapshot_compressed_buf);

        out.snapshot = VFSSnapshot::deserialize(snapshot_str);
    }

    LOG_TRACE(log, "Loaded snapshot {}\nGot log batch\n{}", out.snapshot, fmt::join(log_batch, "\n"));

    out.obsolete_objects = out.snapshot.update(log_batch);

    return out;
}

void ObjectStorageVFSGCThread::writeSnapshot(VFSSnapshot && snapshot, const String & snapshot_name)
{
    LOG_DEBUG(log, "Writing snapshot {}", snapshot_name);

    auto buf = storage.writeFile(snapshot_name, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, {});
    auto compressed_buf = CompressedWriteBuffer{*buf};
    writeString(snapshot.serialize(), compressed_buf);
    compressed_buf.finalize();
    buf->finalize();

    // Local metadata file which we don't need (snapshot will be deleted by replica processing next batch)
    auto tx = storage.metadata_storage->createTransaction();
    tx->unlinkFile(snapshot_name);
    tx->commit();
}

void ObjectStorageVFSGCThread::removeLogEntries(size_t start_logpointer, size_t end_logpointer)
{
    LOG_DEBUG(log, "Removing log range [{};{}]", start_logpointer, end_logpointer);

    const size_t log_batch_length = end_logpointer - start_logpointer + 1;
    Coordination::Requests requests(log_batch_length);
    for (size_t i = 0; i < log_batch_length; ++i)
        requests[i] = zkutil::makeRemoveRequest(getNode(start_logpointer + i), -1);

    storage.zookeeper->multi(requests);
}

String ObjectStorageVFSGCThread::getNode(size_t id) const
{
    // Zookeeper's sequential node is 10 digits with padding zeros
    return fmt::format("{}{:010}", storage.traits.log_item, id);
}
}
