#include "ObjectStorageVFSGCThread.h"
#include "DiskObjectStorageVFS.h"
#include "IO/Lz4DeflatingWriteBuffer.h"
#include "IO/Lz4InflatingReadBuffer.h"
#include "IO/ReadBufferFromEmptyFile.h"
#include "IO/ReadHelpers.h"

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

// TODO myrrc should (possibly?) be settings. The batch size must not be too large as we put it in memory
constexpr size_t batch_min_size = 1, batch_max_size = 5000, snapshot_lz4_compression_level = 8;

ObjectStorageVFSGCThread::ObjectStorageVFSGCThread(DiskObjectStorageVFS & storage_, BackgroundSchedulePool & pool)
    : storage(storage_)
    , log(&Poco::Logger::get(fmt::format("VFSGC({})", storage_.getName())))
    , lock_path(fs::path(storage.traits.locks_node) / "gc_lock")
{
    LOG_DEBUG(log, "GC started with interval {}ms", storage.gc_sleep_ms);
    storage.zookeeper()->createAncestors(lock_path);

    task = pool.createTask(
        log->name(),
        [this]
        {
            try
            {
                run();
            }
            catch (...)
            {
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
            }
            task->scheduleAfter(storage.gc_sleep_ms);
        });
    task->activateAndSchedule();
}

void ObjectStorageVFSGCThread::run() const
{
    using enum Coordination::Error;
    if (auto code = storage.zookeeper()->tryCreate(lock_path, "", zkutil::CreateMode::Ephemeral); code == ZNODEEXISTS)
    {
        LOG_DEBUG(log, "Failed to acquire lock, sleeping");
        return;
    }
    else if (code != ZOK)
        throw Coordination::Exception(code);

    bool successful_run = false;
    SCOPE_EXIT(if (!successful_run) storage.zookeeper()->remove(lock_path));

    Strings log_items_batch = storage.zookeeper()->getChildren(storage.traits.log_base_node);
    if (log_items_batch.size() < batch_min_size)
        return;

    // ZK should return children in lexicographical order. If it were true, we could
    // get minmax by (begin(), rbegin()), but it's not the case so we have to traverse all range
    const auto [start_str, end_str] = std::ranges::minmax(std::move(log_items_batch));
    const size_t start_logpointer = parseFromString<size_t>(start_str.substr(4)); // log- is a prefix
    const size_t end_logpointer = std::min(parseFromString<size_t>(end_str.substr(4)), start_logpointer + batch_max_size);

    LOG_DEBUG(log, "Acquired lock for [{};{}]", start_logpointer, end_logpointer);
    updateSnapshotWithLogEntries(start_logpointer, end_logpointer);
    removeBatch(start_logpointer, end_logpointer);
    LOG_DEBUG(log, "Removed lock for [{};{}]", start_logpointer, end_logpointer);
    successful_run = true;
}

void ObjectStorageVFSGCThread::updateSnapshotWithLogEntries(size_t start_logpointer, size_t end_logpointer) const
{
    auto & object_storage = *storage.object_storage;
    const bool should_have_previous_snapshot = start_logpointer > 0;

    StoredObject old_snapshot = getSnapshotObject(start_logpointer - 1);
    auto old_snapshot_uncompressed_buf = should_have_previous_snapshot
        ? object_storage.readObject(old_snapshot)
        : std::unique_ptr<ReadBufferFromFileBase>(std::make_unique<ReadBufferFromEmptyFile>());
    Lz4InflatingReadBuffer old_snapshot_buf{std::move(old_snapshot_uncompressed_buf)};

    const StoredObject new_snapshot = getSnapshotObject(end_logpointer);
    auto new_snapshot_uncompressed_buf = object_storage.writeObject(new_snapshot, WriteMode::Rewrite);
    // TODO myrrc research zstd dictionary builder or zstd for compression
    Lz4DeflatingWriteBuffer new_snapshot_buf{std::move(new_snapshot_uncompressed_buf), snapshot_lz4_compression_level};

    auto [obsolete, invalid] = getBatch(start_logpointer, end_logpointer).mergeWithSnapshot(old_snapshot_buf, new_snapshot_buf, log);
    if (should_have_previous_snapshot)
        obsolete.emplace_back(std::move(old_snapshot));

    if (!invalid.empty()) // TODO myrrc remove after testing
    {
        String out;
        for (const auto & [path, ref] : invalid)
            fmt::format_to(std::back_inserter(out), "{} {}\n", path, ref);
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid objects:\n{}", out);
    }

    new_snapshot_buf.finalize();
    object_storage.removeObjects(obsolete);
}

VFSLogItem ObjectStorageVFSGCThread::getBatch(size_t start_logpointer, size_t end_logpointer) const
{
    const size_t log_batch_length = end_logpointer - start_logpointer + 1;

    Strings nodes(log_batch_length);
    for (size_t i = 0; i < log_batch_length; ++i)
        nodes[i] = getNode(start_logpointer + i);
    auto responses = storage.zookeeper()->get(nodes);
    nodes = {};

    VFSLogItem out;
    for (size_t i = 0; i < log_batch_length; ++i)
        out.merge(VFSLogItem::parse(responses[i].data));
    LOG_TRACE(log, "Merged batch:\n{}", out);

    return out;
}

void ObjectStorageVFSGCThread::removeBatch(size_t start_logpointer, size_t end_logpointer) const
{
    LOG_DEBUG(log, "Removing log range [{};{}]", start_logpointer, end_logpointer);

    const size_t log_batch_length = end_logpointer - start_logpointer + 1;
    Coordination::Requests requests(log_batch_length + 1);
    for (size_t i = 0; i < log_batch_length; ++i)
        requests[i] = zkutil::makeRemoveRequest(getNode(start_logpointer + i), 0);
    requests[log_batch_length] = zkutil::makeRemoveRequest(lock_path, 0);

    storage.zookeeper()->multi(requests);
}

String ObjectStorageVFSGCThread::getNode(size_t id) const
{
    // Zookeeper's sequential node is 10 digits with padding zeros
    return fmt::format("{}{:010}", storage.traits.log_item, id);
}

StoredObject ObjectStorageVFSGCThread::getSnapshotObject(size_t logpointer) const
{
    /// TODO myrrc this works only for S3ObjectStorage. Must also recheck encrypted disk replication
    return StoredObject{ObjectStorageKey::createAsRelative(storage.object_key_prefix, fmt::format("vfs/_{}", logpointer)).serialize()};
}
}
