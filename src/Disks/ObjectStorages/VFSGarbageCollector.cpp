#include "VFSGarbageCollector.h"
#include <IO/ReadHelpers.h>
#include <IO/S3Common.h>
#include <fmt/chrono.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/ZooKeeper/ZooKeeperLock.h>
#include "DiskObjectStorageVFS.h"
#include "VFSSnapshotIO.h"

namespace ProfileEvents
{
extern const Event VFSGcRunsCompleted;
extern const Event VFSGcRunsException;
extern const Event VFSGcRunsSkipped;
extern const Event VFSGcTotalSeconds;
extern const Event VFSGcCumulativeLogItemsRead;
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

VFSGarbageCollector::VFSGarbageCollector(DiskObjectStorageVFS & disk_, BackgroundSchedulePool & pool)
    : disk(disk_), log(getLogger(fmt::format("VFSGC({})", disk.getName())))
{
    LOG_INFO(log, "GC started");

    createLockNodes(*disk.zookeeper());
    *static_cast<BackgroundSchedulePoolTaskHolder *>(this) = pool.createTask(
        log->name(),
        [this]
        {
            settings = disk.settings.get(); // capture new settings version
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

void VFSGarbageCollector::createLockNodes(Coordination::ZooKeeperWithFaultInjection & zookeeper) const
{
    zookeeper.createAncestors(disk.nodes.gc_lock);
    zookeeper.createIfNotExists(disk.nodes.gc_lock, "");
}

static String generateSnapshotName()
{
    return fmt::format("snapshots/{}", UUIDHelpers::generateV4());
}

void VFSGarbageCollector::run() const
{
    Stopwatch stop_watch;

    // Optimization to reduce probability of clashes between replicas
    // TODO alexfvk: templatize ZooKeeperLock to support ZooKeeperWithFaultInjection
    zkutil::ZooKeeperLock pessimistic_lock(disk.zookeeper()->getKeeper(), disk.nodes.gc_lock, "lock");
    if (!pessimistic_lock.tryLock())
    {
        LOG_DEBUG(log, "Skipped run due to pessimistic lock already acquired");
        return;
    }

    OptimisticLock lock = createLock();
    LOG_DEBUG(log, "GC acquired lock");

    bool successful_run = false, skip_run = false;
    SCOPE_EXIT({
        if (successful_run)
            ProfileEvents::increment(ProfileEvents::VFSGcRunsCompleted);
        else
            ProfileEvents::increment(skip_run ? ProfileEvents::VFSGcRunsSkipped : ProfileEvents::VFSGcRunsException);

        ProfileEvents::increment(ProfileEvents::VFSGcTotalSeconds, stop_watch.elapsedMilliseconds() / 1000);
        LOG_DEBUG(log, "GC iteration finished");
    });

    Strings log_items_batch = disk.zookeeper()->getChildren(disk.nodes.log_base);
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
    const auto [start_str, end_str] = std::ranges::minmax(std::move(log_items_batch));
    constexpr size_t prefix_len = VFSNodes::log_prefix.size();
    const Logpointer start = parseFromString<Logpointer>(start_str.substr(prefix_len));
    const Logpointer end_parsed = parseFromString<Logpointer>(end_str.substr(prefix_len));
    const Logpointer end = std::min(end_parsed, start + settings->batch_max_size);

    if ((skip_run = skipRun(batch_size, start)))
        return;

    LOG_DEBUG(log, "Processing range [{};{}]", start, end);
    const String new_snapshot_name = generateSnapshotName();
    const String old_snapshot_name = std::exchange(lock.snapshot, new_snapshot_name);
    const StoredObject new_snapshot = disk.getMetadataObject(new_snapshot_name);
    const bool old_snapshot_missing = old_snapshot_name.empty();
    const StoredObject old_snapshot = old_snapshot_missing ? StoredObject{} : disk.getMetadataObject(old_snapshot_name);

    updateSnapshotWithLogEntries(start, end, old_snapshot, new_snapshot);

    LOG_DEBUG(log, "Removing log range [{};{}]", start, end);
    if (!releaseLockAndRemoveEntries(std::move(lock), start, end))
    {
        LOG_DEBUG(log, "Skip GC transaction because optimistic lock node was already updated");
        return disk.object_storage->removeObject(new_snapshot);
    }

    if (!old_snapshot_missing)
        disk.object_storage->removeObject(old_snapshot);

    LOG_DEBUG(log, "Removed lock for [{};{}]", start, end);
    successful_run = true;
}

VFSGarbageCollector::OptimisticLock VFSGarbageCollector::createLock() const
{
    auto zookeeper = disk.zookeeper();
    const String & lock = disk.nodes.gc_lock;
    Coordination::Stat stat;
    Coordination::Error error;
    String snapshot;

    using enum Coordination::Error;
    if (zookeeper->tryGet(lock, snapshot, &stat, nullptr, &error))
        return {.snapshot = snapshot, .version = stat.version};
    if (error != ZNONODE)
        throw Coordination::Exception(error);
    createLockNodes(*zookeeper);
    return {.snapshot = zookeeper->get(lock, &stat), .version = stat.version};
}

bool VFSGarbageCollector::skipRun(size_t batch_size, Logpointer start) const
{
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
    if (!disk.zookeeper()->exists(getNode(start), &stat))
    {
        LOG_DEBUG(log, "Skipped run due to {} node loss", start);
        return true;
    }

    using clock = std::chrono::system_clock;
    const size_t delta = std::chrono::duration_cast<ms>(clock::now().time_since_epoch()).count() - stat.mtime;

    if (delta < wait_ms)
        LOG_DEBUG(log, "Skipped run due to insufficient batch size ({} < {}) and time ({} < {})", batch_size, min_size, delta, wait_ms);

    return delta < wait_ms;
}

void VFSGarbageCollector::updateSnapshotWithLogEntries(
    Logpointer start, Logpointer end, const StoredObject & old_snapshot, const StoredObject & new_snapshot) const
{
    const bool old_snapshot_missing = old_snapshot.remote_path.empty();
    if (old_snapshot_missing && start > 0)
        throw Exception(ErrorCodes::INVALID_STATE, "Snapshot is absent but start log pointer is {}", start);

    IObjectStorage & storage = *disk.getObjectStorage();
    VFSSnapshotReadStreamFromString empty{""};
    VFSSnapshotReadStream old_stream{storage, old_snapshot};

    const int level = settings->snapshot_lz4_compression_level;
    auto & old_ref = old_snapshot_missing ? empty : static_cast<IVFSSnapshotReadStream &>(old_stream);
    VFSSnapshotWriteStream new_stream{storage, new_snapshot, level};

    auto [obsolete, invalid] = getBatch(start, end).mergeWithSnapshot(old_ref, new_stream, &*log);

    new_stream.finalize();
    disk.object_storage->removeObjectsIfExist(obsolete);

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
    const size_t batch_len = end - start + 1;
    Strings nodes(batch_len);
    for (size_t i = 0; i < batch_len; ++i)
        nodes[i] = getNode(start + i);
    auto responses = disk.zookeeper()->get(nodes);
    nodes = {};

    ProfileEvents::increment(ProfileEvents::VFSGcCumulativeLogItemsRead, batch_len);

    VFSLogItem out;
    for (size_t i = 0; i < batch_len; ++i)
        out.merge(VFSLogItem::parse(responses[i].data));
    LOG_TRACE(log, "Merged batch:\n{}", out);

    return out;
}

String VFSGarbageCollector::getNode(Logpointer ptr) const
{
    // Zookeeper's sequential node is 10 digits with padding zeros
    return fmt::format("{}{:010}", disk.nodes.log_item, ptr);
}

bool VFSGarbageCollector::releaseLockAndRemoveEntries(VFSGarbageCollector::OptimisticLock && lock, Logpointer start, Logpointer end) const
{
    fiu_do_on(FailPoints::vfs_gc_optimistic_lock_delay, {
        const ms sleep_time(settings->gc_sleep_ms);
        LOG_DEBUG(log, "Failpoint vfs_gc_optimistic_lock_delay sleeping {}", sleep_time);
        std::this_thread::sleep_for(sleep_time);
    });

    auto zookeeper = disk.zookeeper();

    const size_t batch_len = end - start + 1;
    Coordination::Requests ops(batch_len + 1);
    // As it's first command, returned _code_ may be ZBADVERSION
    ops[0] = zkutil::makeSetRequest(disk.nodes.gc_lock, lock.snapshot, lock.version);
    for (size_t i = 0; i < batch_len; ++i)
        ops[i + 1] = zkutil::makeRemoveRequest(getNode(start + i), -1);

    Coordination::Responses res;
    using enum Coordination::Error;
    if (const auto code = zookeeper->tryMulti(ops, res); code == ZOK)
        return true;
    else if (code == ZBADVERSION)
        return false;
    else
        throw Coordination::Exception(code);
}
}
