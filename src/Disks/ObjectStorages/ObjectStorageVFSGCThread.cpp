#include "ObjectStorageVFSGCThread.h"
#include <ranges>
#include "Common/ZooKeeper/ZooKeeperLock.h"
#include "Compression/CompressedReadBuffer.h"
#include "Compression/CompressedWriteBuffer.h"
#include "Disks/ObjectStorages/DiskObjectStorageVFS.h"
#include "IO/ReadBufferFromEmptyFile.h"
#include "IO/ReadBufferFromString.h"
#include "Interpreters/Context.h"

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
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

    Strings log_items_batch = storage.zookeeper->getChildren(storage.traits.log_base_node);
    constexpr size_t batch_min_size = 1; // TODO myrrc should be a setting
    // TODO myrrc should (possibly?) be a setting. The batch size must not be too large
    // as we put it in memory
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
    String snapshot_remote_path = updateSnapshotWithLogEntries(start_logpointer, end_logpointer);
    onBatchProcessed(start_logpointer, end_logpointer, snapshot_remote_path);

    LOG_DEBUG(log, "Removed lock for log range [{};{}]", start_logpointer, end_logpointer);
}

String ObjectStorageVFSGCThread::updateSnapshotWithLogEntries(size_t start_logpointer, size_t end_logpointer)
{
    const bool should_have_previous_snapshot = start_logpointer > 0;

    // TODO myrrc what if replica capturing log entry doesn't get correct snapshot due to stale read?
    // Should write snapshot logpointer here, read first child of log, and if lp + 1 != first_child issue
    // storage.zookeeper->sync(storage.traits.log_base_node)
    auto old_snapshot_uncompressed_buf = should_have_previous_snapshot
        ? storage.object_storage->readObject(StoredObject{storage.zookeeper->get(storage.traits.last_snapshot_node)})
        : std::unique_ptr<ReadBufferFromFileBase>(std::make_unique<ReadBufferFromEmptyFile>());

    CompressedReadBuffer old_snapshot_buf{*old_snapshot_uncompressed_buf};

    // TODO myrrc use snapshot name instead of random name to locate objects on s3 without involving clickhouse
    const String new_snapshot_name = fmt::format("vfs_snapshot_{}", end_logpointer);
    const ObjectStorageKey new_snapshot_key = storage.object_storage->generateObjectKeyForPath(new_snapshot_name);
    StoredObject new_snapshot{new_snapshot_key.serialize()};

    auto new_snapshot_uncompressed_buf = storage.object_storage->writeObject(new_snapshot, WriteMode::Rewrite);
    CompressedWriteBuffer new_snapshot_buf{*new_snapshot_uncompressed_buf};

    VFSLogItem batch = getBatch(start_logpointer, end_logpointer);
    StoredObjects obsolete = mergeSnapshotWithLogBatch(old_snapshot_buf, std::move(batch), new_snapshot_buf);

    new_snapshot_buf.finalize();
    new_snapshot_uncompressed_buf->finalize();

    String snapshot_remote_path = std::move(new_snapshot.remote_path);

    LOG_TRACE(log, "Removing obsolete objects: {}", fmt::join(obsolete, "\n"));
    storage.object_storage->removeObjects(obsolete);

    return snapshot_remote_path;
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
        auto item = parseFromString<VFSLogItem>(responses[i].data);
        LOG_TRACE(log, "Log item {}", item);
        out.merge(std::move(item));
    }
    LOG_TRACE(log, "Merged batch {}", out);

    return out;
}

StoredObjects ObjectStorageVFSGCThread::mergeSnapshotWithLogBatch(ReadBuffer & snapshot, VFSLogItem && batch, WriteBuffer & new_snapshot)
{
    /// Both snapshot and batch data are sorted so we can merge them in one traversal
    StoredObjects obsolete;
    using Pair = std::pair<String, int>;

    auto read_snapshot_item = [&]
    {
        Pair out;
        readStringUntilWhitespaceInto(out.first, snapshot);
        readIntTextUnsafe(out.second, snapshot);
        return out;
    };

    Pair left;
    if (!snapshot.eof())
        left = read_snapshot_item();
    auto batch_it = batch.begin();

    while (!snapshot.eof() && batch_it != batch.cend())
    {
        auto [left_remote, left_links] = left;
        auto [right_remote, right_links] = *batch_it;

        if (const int res = left_remote.compare(right_remote); res == 0)
        { // TODO myrrc <=>
            if (int delta = left_links + right_links; delta == 0)
                obsolete.emplace_back(StoredObject{left_remote});
            // TODO myrrc collect invalid objects instead of throwing
            else if (delta < 0)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "{} references to {}", delta, left_remote);
            else
                writeString(fmt::format("{} {}\n", left_remote, delta), new_snapshot);

            left = read_snapshot_item();
            ++batch_it;
        }
        else if (res < 0)
        {
            writeString(fmt::format("{} {}\n", left_remote, left_links), new_snapshot);
            left = read_snapshot_item();
        }
        else
        {
            writeString(fmt::format("{} {}\n", right_remote, right_links), new_snapshot);
            ++batch_it;
        }
    }

    while (!snapshot.eof())
    {
        auto [left_remote, left_links] = read_snapshot_item();
        writeString(fmt::format("{} {}\n", left_remote, left_links), new_snapshot);
    }

    while (batch_it != batch.cend())
    {
        auto [right_remote, right_links] = *batch_it;
        writeString(fmt::format("{} {}\n", right_remote, right_links), new_snapshot);
        ++batch_it;
    }

    return obsolete;
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
