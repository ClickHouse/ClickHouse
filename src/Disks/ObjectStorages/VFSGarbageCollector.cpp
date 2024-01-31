#include "VFSGarbageCollector.h"
#include "Common/ProfileEvents.h"
#include "Common/Stopwatch.h"
#include "DiskObjectStorageVFS.h"
#include "IO/Lz4DeflatingWriteBuffer.h"
#include "IO/Lz4InflatingReadBuffer.h"
#include "IO/ReadHelpers.h"
#include "IO/S3Common.h"
#if USE_AZURE_BLOB_STORAGE
#    include <azure/storage/common/storage_exception.hpp>
#endif

namespace ProfileEvents
{
extern const Event VFSGcRunsCompleted;
extern const Event VFSGcRunsException;
extern const Event VFSGcRunsSkipped;
extern const Event VFSGcTotalMicroseconds; // TODO myrrc switch to seconds?
extern const Event VFSGcCumulativeSnapshotBytesRead;
extern const Event VFSGcCumulativeLogItemsRead;
}

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

VFSGarbageCollector::VFSGarbageCollector(DiskObjectStorageVFS & storage_, BackgroundSchedulePool & pool)
    : storage(storage_), log(getLogger(fmt::format("VFSGC({})", storage_.getName())))
{
    LOG_INFO(log, "GC started");

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

using Logpointer = VFSGarbageCollector::Logpointer;

const int EPHEMERAL = zkutil::CreateMode::Ephemeral;
void VFSGarbageCollector::run() const
{
    Stopwatch stop_watch;

    using enum Coordination::Error;
    const String & lock_path = storage.traits.gc_lock_path;
    if (const auto code = storage.zookeeper()->tryCreate(lock_path, "", EPHEMERAL); code == ZNODEEXISTS)
    {
        LOG_DEBUG(log, "Failed to acquire lock, sleeping");
        return;
    }
    else if (code == ZNONODE)
    {
        LOG_ERROR(log, "{} not found, will recreate ZooKeeper nodes", storage.traits.gc_lock_path);
        return storage.createNodes();
    }
    else if (code != ZOK)
        throw Coordination::Exception(code);
    LOG_DEBUG(log, "Acquired lock");

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

    Strings log_items_batch = storage.zookeeper()->getChildren(storage.traits.log_base_node);
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
    const Logpointer start = parseFromString<Logpointer>(start_str.substr(4)); // log- is a prefix
    const Logpointer end_parsed = parseFromString<Logpointer>(end_str.substr(4));
    const Logpointer end = std::min(end_parsed, start + settings->batch_max_size);

    if ((skip_run = skipRun(batch_size, start, end)))
        return;

    LOG_DEBUG(log, "Processing range [{};{}]", start, end);
    updateSnapshotWithLogEntries(start, end);
    removeBatch(start, end);
    LOG_DEBUG(log, "Removed lock for [{};{}]", start, end);
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

void VFSGarbageCollector::updateSnapshotWithLogEntries(Logpointer start, Logpointer end) const
{
    IObjectStorage & object_storage = *storage.object_storage;
    const size_t start_regarding_zero = start == 0 ? 0 : (start - 1);
    StoredObject old_snapshot;
    std::optional<Lz4InflatingReadBuffer> old_snapshot_buf;

    auto populate_old_snapshot = [&](Logpointer target_start)
    {
        old_snapshot = getSnapshotObject(target_start);
        auto uncompressed_buf = object_storage.readObject(old_snapshot);
        old_snapshot_buf.emplace(std::move(uncompressed_buf));
    };

    try
    {
        populate_old_snapshot(start_regarding_zero);
        old_snapshot_buf->eof(); // throws if file not found
    }
    catch (std::exception & e)
    {
        const Logpointer new_start = reconcile(start_regarding_zero, end, std::move(e));
        if (new_start == end)
            return;
        // TODO myrrc if there are multiple candidates, shouldn't we remove all of them?
        LOG_DEBUG(log, "Selected snapshot {} as best candidate", new_start);
        populate_old_snapshot(new_start);
    }

    const StoredObject new_snapshot = getSnapshotObject(end);
    auto uncompressed_buf = object_storage.writeObject(new_snapshot, WriteMode::Rewrite);
    // TODO myrrc research zstd dictionary builder or zstd for compression
    Lz4DeflatingWriteBuffer new_snapshot_buf{std::move(uncompressed_buf), settings->snapshot_lz4_compression_level};

    auto [obsolete, invalid] = getBatch(start, end).mergeWithSnapshot(*old_snapshot_buf, new_snapshot_buf, &*log);
    obsolete.emplace_back(std::move(old_snapshot));

    ProfileEvents::increment(ProfileEvents::VFSGcCumulativeLogItemsRead, end - start);
    ProfileEvents::increment(ProfileEvents::VFSGcCumulativeSnapshotBytesRead, old_snapshot_buf->count());

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

#pragma clang diagnostic ignored "-Wmissing-noreturn" // Conditional [[noreturn]] doesn't exist
static void check404(Poco::Logger * const log, std::exception & e)
{
    LOG_DEBUG(log, "Checking snapshot read exception {}", e.what());
    if (false) // TODO myrrc this works only for s3 and azure
    {
    }
#if USE_AWS_S3
    else if (auto * e_s3 = typeid_cast<S3Exception *>(&e))
    {
        if (e_s3->getS3ErrorCode() != Aws::S3::S3Errors::NO_SUCH_KEY)
            throw e;
    }
#endif
#if USE_AZURE_BLOB_STORAGE
    else if (auto * e_azure = typeid_cast<Azure::Storage::StorageException *>(&e))
    {
        if (e_azure->StatusCode != Azure::Core::Http::HttpStatusCode::NotFound)
            throw e;
    }
#endif
    else
        throw e;
}

constexpr std::string_view SNAPSHOTS_PATH = "/snapshots";
// Better approach would be to pass Exception && e, but Azure StorageException doesn't derive from it
Logpointer VFSGarbageCollector::reconcile(Logpointer start, Logpointer end, std::exception && e) const
{
    check404(log.get(), e);
    const bool starting = start == 0;
    if (!starting)
        LOG_WARNING(log, "Snapshot for {} not found", start);

    const fs::path snapshots_path = storage.getMetadataObject(SNAPSHOTS_PATH).remote_path;
    RelativePathsWithMetadata snapshots;
    constexpr int max_candidates = 5;
    storage.object_storage->listObjects(snapshots_path, snapshots, max_candidates);

    if (const bool empty = snapshots.empty(); empty && starting)
    {
        LOG_DEBUG(log, "Didn't find snapshot before start, writing empty file");
        const StoredObject object = getSnapshotObject(0);
        auto buf = storage.object_storage->writeObject(object, WriteMode::Rewrite);
        Lz4DeflatingWriteBuffer{std::move(buf), settings->snapshot_lz4_compression_level}.finalize();
        return 0;
    }
    else if (empty)
    {
        LOG_ERROR(log, "Didn't find any snapshots in {}", snapshots_path);
        throw e;
    }

    std::vector<Logpointer> candidates;
    auto parse = [&](const RelativePathWithMetadata & obj)
    { return parseFromString<Logpointer>(fs::path(obj.relative_path).lexically_relative(snapshots_path).string()); };
    std::ranges::transform(std::move(snapshots), std::back_inserter(candidates), std::move(parse));
    std::ranges::sort(candidates);
    LOG_DEBUG(log, "Snapshot candidates: [{}]", fmt::join(candidates, ", "));

    if (const auto it = std::ranges::find(candidates, end); it != candidates.end())
    {
        LOG_INFO(log, "Found leftover from previous GC run, discarding batch [{};{}]", start, end);
        return end;
    }

    // First run after Zookeeper loss without backups (~ node sequential counter reset) but we have snapshot
    if (const Logpointer greatest_end = *candidates.rbegin(); starting && greatest_end > 0)
        return greatest_end;
    else if (const auto it = std::ranges::lower_bound(candidates, start); it != candidates.begin())
        return it == candidates.end() ? greatest_end : *std::prev(it);

    throw e; // TODO myrrc add more heuristics. Current approach may be too conservative
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

void VFSGarbageCollector::removeBatch(Logpointer start, Logpointer end) const
{
    LOG_DEBUG(log, "Removing log range [{};{}]", start, end);

    const size_t log_batch_length = end - start + 1;
    Coordination::Requests requests(log_batch_length + 1);
    for (size_t i = 0; i < log_batch_length; ++i)
        requests[i] = zkutil::makeRemoveRequest(getNode(start + i), 0);
    requests[log_batch_length] = zkutil::makeRemoveRequest(storage.traits.gc_lock_path, 0);

    storage.zookeeper()->multi(requests);
}

String VFSGarbageCollector::getNode(Logpointer ptr) const
{
    // Zookeeper's sequential node is 10 digits with padding zeros
    return fmt::format("{}{:010}", storage.traits.log_item, ptr);
}

StoredObject VFSGarbageCollector::getSnapshotObject(Logpointer ptr) const
{
    // We need a separate folder to quickly get snapshot with unknown logpointer on reconciliation
    return storage.getMetadataObject(fmt::format("{}/{}", SNAPSHOTS_PATH, ptr));
}
}
