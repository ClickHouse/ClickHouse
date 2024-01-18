#include "ObjectStorageVFSGCThread.h"
#include "Common/ProfileEvents.h"
#include "Common/Stopwatch.h"
#include "DiskObjectStorageVFS.h"
#include "IO/Lz4DeflatingWriteBuffer.h"
#include "IO/Lz4InflatingReadBuffer.h"
#include "IO/ReadBufferFromEmptyFile.h"
#include "IO/ReadHelpers.h"

namespace ProfileEvents
{
extern const Event VFSGcRunsCompleted;
extern const Event VFSGcRunsException;
extern const Event VFSGcRunsSkipped;
extern const Event VFSGcTotalMicroseconds;
extern const Event VFSGcCumulativeSnapshotBytesRead;
extern const Event VFSGcCumulativeLogItemsRead;
}

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

ObjectStorageVFSGCThread::ObjectStorageVFSGCThread(DiskObjectStorageVFS & storage_, BackgroundSchedulePool & pool)
    : storage(storage_)
    , log(&Poco::Logger::get(fmt::format("VFSGC({})", storage_.getName())))
    , lock_path(fs::path(storage.settings.locks_node) / "gc_lock")
{
    LOG_INFO(log, "GC started");
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
            task->scheduleAfter(storage.settings.gc_sleep_ms);
        });
    task->activateAndSchedule();
}

void ObjectStorageVFSGCThread::run() const
{
    Stopwatch stop_watch;

    using enum Coordination::Error;
    if (auto code = storage.zookeeper()->tryCreate(lock_path, "", zkutil::CreateMode::Ephemeral); code == ZNODEEXISTS)
    {
        LOG_DEBUG(log, "Failed to acquire lock, sleeping");
        return;
    }
    else if (code != ZOK)
        throw Coordination::Exception(code);

    bool successful_run = false, skip_run = false;
    SCOPE_EXIT({
        if (successful_run)
            ProfileEvents::increment(ProfileEvents::VFSGcRunsCompleted);
        else
        {
            ProfileEvents::increment(skip_run ? ProfileEvents::VFSGcRunsSkipped : ProfileEvents::VFSGcRunsException);
            storage.zookeeper()->remove(lock_path);
        };
        ProfileEvents::increment(ProfileEvents::VFSGcTotalMicroseconds, stop_watch.elapsedMicroseconds());
    });

    Strings log_items_batch = storage.zookeeper()->getChildren(storage.settings.log_base_node);
    const size_t batch_size = log_items_batch.size();
    if ((skip_run = log_items_batch.empty()))
    {
        LOG_DEBUG(log, "Skipped run due to empty batch");
        return;
    }

    // TODO myrrc Sequential node in zookeeper overflows after 32 bit.
    // We can catch this case by checking (end_logpointer - start_logpointer) != log_items_batch.size()
    // In that case we should find the overflow point and process only the part before overflow
    // (so next GC could capture the range with increasing logpointers).
    // We also must use a signed type for logpointers.
    const auto [start_str, end_str] = std::ranges::minmax(std::move(log_items_batch));
    const size_t start_logpointer = parseFromString<size_t>(start_str.substr(4)); // log- is a prefix
    const size_t end_logpointer = std::min(parseFromString<size_t>(end_str.substr(4)), start_logpointer + storage.settings.batch_max_size);

    if ((skip_run = skipRun(batch_size, start_logpointer)))
        return;

    LOG_DEBUG(log, "Acquired lock for [{};{}]", start_logpointer, end_logpointer);
    // TODO myrrc store for 1 iteration more in case batch removal failed. Next node should try to
    // load previous snapshot
    updateSnapshotWithLogEntries(start_logpointer, end_logpointer);
    removeBatch(start_logpointer, end_logpointer);
    LOG_DEBUG(log, "Removed lock for [{};{}]", start_logpointer, end_logpointer);
    successful_run = true;
}

bool ObjectStorageVFSGCThread::skipRun(size_t batch_size, size_t log_pointer) const
{
    const UInt64 min_size = storage.settings.batch_min_size;
    if (batch_size >= min_size)
        return false;

    const Int64 wait_ms = storage.settings.batch_can_wait_ms;
    if (!wait_ms)
    {
        LOG_DEBUG(log, "Skipped run due to insufficient batch size: {} < {}", batch_size, min_size);
        return true;
    }

    Coordination::Stat stat;
    storage.zookeeper()->exists(getNode(log_pointer), &stat);

    const auto delta
        = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count() - stat.mtime;

    if (delta < wait_ms)
        LOG_DEBUG(log, "Skipped run due to insufficient batch size ({} < {}) and time ({} < {})", batch_size, min_size, delta, wait_ms);

    return delta < wait_ms;
}


void ObjectStorageVFSGCThread::updateSnapshotWithLogEntries(size_t start_logpointer, size_t end_logpointer) const
{
    ProfileEvents::increment(ProfileEvents::VFSGcCumulativeLogItemsRead, end_logpointer - start_logpointer);

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
    Lz4DeflatingWriteBuffer new_snapshot_buf{std::move(new_snapshot_uncompressed_buf), storage.settings.snapshot_lz4_compression_level};

    auto [obsolete, invalid] = getBatch(start_logpointer, end_logpointer).mergeWithSnapshot(old_snapshot_buf, new_snapshot_buf, log);
    if (should_have_previous_snapshot)
    {
        obsolete.emplace_back(std::move(old_snapshot));
        ProfileEvents::increment(ProfileEvents::VFSGcCumulativeSnapshotBytesRead, old_snapshot_buf.count());
    }

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
    return fmt::format("{}{:010}", storage.settings.log_item, id);
}

StoredObject ObjectStorageVFSGCThread::getSnapshotObject(size_t logpointer) const
{
    return storage.getMetadataObject(fmt::format("{}", logpointer));
}
}
