#include "VFSGarbageCollector.h"

#include <Disks/ObjectStorages/DiskObjectStorageVFS.h>
#include <IO/S3Common.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/ZooKeeper/ZooKeeperLock.h>


namespace ProfileEvents
{
extern const Event VFSGcRunsCompleted;
extern const Event VFSGcRunsException;
extern const Event VFSGcRunsSkipped;
extern const Event VFSGcTotalMicroseconds; // TODO myrrc switch to seconds?
extern const Event VFSGcCumulativeLogItemsRead;
}

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace FailPoints
{
extern const char vfs_gc_optimistic_lock_delay[];
}


VFSGarbageCollector::VFSGarbageCollector(DiskObjectStorageVFS & storage_, BackgroundSchedulePool & pool)
    : storage(storage_), snapshot_storage(storage_.snapshot_storage), log(getLogger(fmt::format("VFSGC({})", storage_.getName())))
{
    LOG_INFO(log, "GC started");

    createLockNodes(storage.zookeeper());
    *static_cast<BackgroundSchedulePoolTaskHolder *>(this) = pool.createTask(
        log->name(),
        [this]
        {
            settings = storage.settings.get(); // update each run to capture new settings
            try
            {
                run();
            }
            catch (...)
            {
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
            }
            (*this)->scheduleAfter(settings->gc_sleep_ms);
        });
    (*this)->activateAndSchedule();
}

void VFSGarbageCollector::createLockNodes(FaultyKeeper zookeeper) const
{
    zookeeper->createAncestors(storage.nodes.gc_lock);
    zookeeper->createIfNotExists(storage.nodes.gc_lock, "");
}

VFSGarbageCollector::LockNode VFSGarbageCollector::getOptimisticLock() const
{
    auto zookeeper = storage.zookeeper();
    const String & gc_lock_path = storage.nodes.gc_lock;

    Coordination::Stat stat;
    String value;

    try
    {
        value = zookeeper->get(gc_lock_path, &stat);
    }
    catch (const Coordination::Exception & e)
    {
        if (e.code == Coordination::Error::ZNONODE)
        {
            createLockNodes(zookeeper);
            value = zookeeper->get(gc_lock_path, &stat);
        }
        else
            throw;
    }

    return VFSGarbageCollector::LockNode{.snapshot = value, .version = stat.version};
}

bool VFSGarbageCollector::releaseOptimisticLock(const VFSGarbageCollector::LockNode & lock_node, Coordination::Requests & ops) const
{
    auto zookeeper = storage.zookeeper();
    ops.insert(ops.begin(), zkutil::makeSetRequest(storage.nodes.gc_lock, lock_node.snapshot, lock_node.version));
    try
    {
        zookeeper->multi(ops);
    }
    catch (const Coordination::Exception & e)
    {
        if (e.code == Coordination::Error::ZBADVERSION)
            return false;
        throw;
    }
    return true;
}

String VFSGarbageCollector::generateSnapshotName() const
{
    return fmt::format("snapshot_{}", UUIDHelpers::generateV4());
}

void VFSGarbageCollector::cleanSnapshots(const String & current_snapshot, const Strings & all_snapshots) const
{
    Strings obsolete_snapshots;
    std::ranges::remove_copy(all_snapshots, std::back_inserter(obsolete_snapshots), current_snapshot);

    if (!obsolete_snapshots.empty())
        snapshot_storage->removeSnapshots(obsolete_snapshots);
}

void VFSGarbageCollector::run() const
{
    Stopwatch stop_watch;

    // Acquire pessimistic lock here as an optimization
    // to reduce the probability of clashes between replicas, but not rely on it
    // TODO: templatize ZooKeeperLock to support ZooKeeperWithFaultInjection
    zkutil::ZooKeeperLock lock(storage.zookeeper()->getKeeper(), storage.nodes.gc_lock, "lock");
    if (!lock.tryLock())
    {
        LOG_DEBUG(log, "Skipped run due to pessimistic lock is already acquired");
        return;
    }

    auto lock_node = getOptimisticLock();
    LOG_DEBUG(log, "GC acquired optimistic lock");

    bool successful_run = false, skip_run = false;
    SCOPE_EXIT({
        if (successful_run)
            ProfileEvents::increment(ProfileEvents::VFSGcRunsCompleted);
        else
            ProfileEvents::increment(skip_run ? ProfileEvents::VFSGcRunsSkipped : ProfileEvents::VFSGcRunsException);

        ProfileEvents::increment(ProfileEvents::VFSGcTotalMicroseconds, stop_watch.elapsedMicroseconds());
        LOG_DEBUG(log, "GC iteration finished");
    });

    Strings log_items_batch = storage.zookeeper()->getChildren(storage.nodes.log_base);
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
    // We also must use a signed type for logpointers (and carefully check overflows)
    constexpr std::string_view zoo_seq_prefix = "log-";
    const auto [start_str, end_str] = std::ranges::minmax(std::move(log_items_batch));
    const Logpointer start = parseFromString<Logpointer>(start_str.substr(zoo_seq_prefix.length()));
    const Logpointer end_parsed = parseFromString<Logpointer>(end_str.substr(zoo_seq_prefix.length()));
    const Logpointer end = std::min(end_parsed, start + settings->batch_max_size);

    if ((skip_run = skipRun(batch_size, start, end)))
        return;

    if (lock_node.snapshot.empty())
    {
        if (start == 0)
        {
            LOG_DEBUG(log, "Write initial empty snapshot");
            lock_node.snapshot = generateSnapshotName();
            auto initial_snapshot = snapshot_storage->writeSnapshot(lock_node.snapshot);
            initial_snapshot->finalize();
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Snapshot is absent but start log pointer is {}. Full migration is needed", start);
    }
    LOG_DEBUG(log, "Processing range [{};{}]", start, end);

    // Write new snapshot to the storage
    String old_snapshot_name = lock_node.snapshot;
    String new_snapshot_name = generateSnapshotName();
    updateSnapshotWithLogEntries(start, end, old_snapshot_name, new_snapshot_name);
    lock_node.snapshot = new_snapshot_name;

    // Collect all snapshots name to clean after successful lock releasing
    auto all_snapshots = snapshot_storage->listSnapshots();

    // Failpoint for testing purpose
    fiu_do_on(FailPoints::vfs_gc_optimistic_lock_delay, {
        std::chrono::milliseconds sleep_time(settings->gc_sleep_ms);
        LOG_DEBUG(log, "Failpoint vfs_gc_optimistic_lock_delay sleeping {} ms", settings->gc_sleep_ms);
        std::this_thread::sleep_for(sleep_time);
    });

    LOG_DEBUG(log, "Removing log range [{};{}]", start, end);
    auto requests = makeRemoveBatchRequests(start, end);
    if (!releaseOptimisticLock(lock_node, requests))
    {
        LOG_DEBUG(log, "Skip GC transaction because optimistic lock node was already updated");
        return;
    }
    LOG_DEBUG(log, "Removed lock for [{};{}]", start, end);

    // We can remove snapshots safely because the list was obtained before releasing lock
    cleanSnapshots(new_snapshot_name, all_snapshots);
    successful_run = true;
}

bool VFSGarbageCollector::skipRun(size_t batch_size, Logpointer start, Logpointer end) const
{
    // We have snapshot with name "0" either if 1. No items have been processed (needed for migrations),
    // 2. We processed a batch of single item with logpointer 0.
    // Skip as otherwise we'd read from 0 and write to 0 at same time which would lead to file corruption.
    if (start == 0 && end == 0)
        return true;

    const size_t min_size = settings->batch_min_size;
    if (batch_size >= min_size)
        return false;

    const size_t wait_ms = settings->batch_can_wait_ms;
    if (!wait_ms)
    {
        LOG_DEBUG(log, "Skipped run due to insufficient batch size: {} < {}", batch_size, min_size);
        return true;
    }

    Coordination::Stat stat;
    storage.zookeeper()->exists(getNode(start), &stat);

    using ms = std::chrono::milliseconds;
    using clock = std::chrono::system_clock;
    const size_t delta = std::chrono::duration_cast<ms>(clock::now().time_since_epoch()).count() - stat.mtime;

    if (delta < wait_ms)
        LOG_DEBUG(log, "Skipped run due to insufficient batch size ({} < {}) and time ({} < {})", batch_size, min_size, delta, wait_ms);

    return delta < wait_ms;
}

void VFSGarbageCollector::updateSnapshotWithLogEntries(
    Logpointer start, Logpointer end, const String & old_snapshot_name, const String & new_snapshot_name) const
{
    LOG_DEBUG(log, "updateSnapshotWithLogEntries start: {}  end: {}", start, end);
    IObjectStorage & object_storage = *storage.object_storage;
    VFSSnapshotReadStreamPtr old_snapshot_stream;

    old_snapshot_stream = snapshot_storage->readSnapshot(old_snapshot_name);
    auto new_snapshot_stream = snapshot_storage->writeSnapshot(new_snapshot_name);
    auto [obsolete, invalid] = getBatch(start, end).mergeWithSnapshot(*old_snapshot_stream, *new_snapshot_stream, &*log);

    ProfileEvents::increment(ProfileEvents::VFSGcCumulativeLogItemsRead, end - start);

    new_snapshot_stream->finalize();
    object_storage.removeObjectsIfExist(obsolete);

    if (!invalid.empty()) // TODO myrrc remove after testing
    {
        String out;
        for (const auto & [path, ref] : invalid)
            fmt::format_to(std::back_inserter(out), "{} {}\n", path, ref);
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid objects:\n{}", out);
    }
}

VFSLogItem VFSGarbageCollector::getBatch(Logpointer start, Logpointer end) const
{
    const size_t log_batch_length = end - start + 1;

    Strings nodes(log_batch_length);
    for (size_t i = 0; i < log_batch_length; ++i)
        nodes[i] = getNode(start + i);
    auto responses = storage.zookeeper()->get(nodes);
    nodes = {};

    VFSLogItem out;
    for (size_t i = 0; i < log_batch_length; ++i)
        out.merge(VFSLogItem::parse(responses[i].data));
    LOG_TRACE(log, "Merged batch:\n{}", out);

    return out;
}

Coordination::Requests VFSGarbageCollector::makeRemoveBatchRequests(Logpointer start, Logpointer end) const
{
    const size_t log_batch_length = end - start + 1; // Including the last item
    Coordination::Requests requests(log_batch_length);

    for (size_t i = 0; i < log_batch_length; ++i)
        requests[i] = zkutil::makeRemoveRequest(getNode(start + i), -1);

    return requests;
}

String VFSGarbageCollector::getNode(Logpointer ptr) const
{
    // Zookeeper's sequential node is 10 digits with padding zeros
    return fmt::format("{}{:010}", storage.nodes.log_item, ptr);
}
}
