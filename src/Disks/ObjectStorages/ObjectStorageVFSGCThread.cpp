#include "ObjectStorageVFSGCThread.h"
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

ObjectStorageVFSGCThread::ObjectStorageVFSGCThread(DiskObjectStorageVFS & storage_, BackgroundSchedulePool & pool)
    : storage(storage_), log(&Poco::Logger::get(fmt::format("VFSGC({})", storage_.getName())))
{
    storage.zookeeper()->createAncestors(storage.traits.gc_lock_path);

    LOG_INFO(log, "GC started");

    task = pool.createTask(
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
            task->scheduleAfter(settings->gc_sleep_ms);
        });
    task->activateAndSchedule();
}

using Logpointer = ObjectStorageVFSGCThread::Logpointer;

const int EPHEMERAL = zkutil::CreateMode::Ephemeral;
void ObjectStorageVFSGCThread::run()
{
    Stopwatch stop_watch;

    using enum Coordination::Error;
    if (auto code = storage.zookeeper()->tryCreate(storage.traits.gc_lock_path, "", EPHEMERAL); code == ZNODEEXISTS)
    {
        LOG_DEBUG(log, "Failed to acquire lock, sleeping");
        return;
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
            storage.zookeeper()->remove(storage.traits.gc_lock_path);
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
    if (start == 0)
        tryWriteSnapshotForZero();

    LOG_DEBUG(log, "Processing range [{};{}]", start, end);
    updateSnapshotWithLogEntries(start, end);
    removeBatch(start, end);
    LOG_DEBUG(log, "Removed lock for [{};{}]", start, end);
    successful_run = true;
}

bool ObjectStorageVFSGCThread::skipRun(size_t batch_size, Logpointer start, Logpointer end) const
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

void ObjectStorageVFSGCThread::tryWriteSnapshotForZero() const
{
    // On start, we may or may not have snapshot for state before processing first log item
    const StoredObject object = getSnapshotObject(0);
    if (storage.object_storage->exists(object))
        return;
    LOG_DEBUG(log, "Didn't find snapshot for 0, writing empty file");
    auto buf = storage.object_storage->writeObject(object, WriteMode::Rewrite);
    Lz4DeflatingWriteBuffer{std::move(buf), settings->snapshot_lz4_compression_level}.finalize();
}

void ObjectStorageVFSGCThread::updateSnapshotWithLogEntries(Logpointer start, Logpointer end) const
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
    catch (Exception & e)
    {
        const Logpointer new_start = reconcileLogWithSnapshot(start_regarding_zero, end, std::move(e));
        if (new_start == end)
            return;
        populate_old_snapshot(new_start);
    }

    const StoredObject new_snapshot = getSnapshotObject(end);
    auto uncompressed_buf = object_storage.writeObject(new_snapshot, WriteMode::Rewrite);
    // TODO myrrc research zstd dictionary builder or zstd for compression
    Lz4DeflatingWriteBuffer new_snapshot_buf{std::move(uncompressed_buf), settings->snapshot_lz4_compression_level};

    auto [obsolete, invalid] = getBatch(start, end).mergeWithSnapshot(*old_snapshot_buf, new_snapshot_buf, log);
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

static void check404(Exception && e)
{
    // TODO myrrc this works only for s3 and azure
    if (false)
    {
    }
#if USE_AWS_S3
    else if (auto * e_s3 = typeid_cast<S3Exception *>(&e))
    {
        if (e_s3->getS3ErrorCode() != Aws::S3::S3Errors::NO_SUCH_KEY)
            throw std::move(e);
    }
#endif
#if USE_AZURE_BLOB_STORAGE
    else if (auto * e_azure = typeid_cast<Azure::Storage::StorageException *>(&e))
    {
        if (e_azure->StatusCode != Azure::Core::Http::HttpStatusCode::NotFound)
            throw std::move(e);
    }
#endif
    else
        throw std::move(e);
}

constexpr std::string_view SNAPSHOTS_PATH = "/snapshots";
Logpointer ObjectStorageVFSGCThread::reconcileLogWithSnapshot(Logpointer start, Logpointer end, Exception && e) const
{
    check404(std::move(e));
    LOG_WARNING(log, "Snapshot for {} not found", start);

    const String snapshots_folder = storage.getMetadataObject(SNAPSHOTS_PATH).remote_path;
    RelativePathsWithMetadata snapshots;
    if (storage.object_storage->listObjects(snapshots_folder, snapshots, 5); snapshots.empty())
    {
        LOG_ERROR(log, "Did not find any snapshots in {}", snapshots_folder);
        throw std::move(e);
    }
    std::vector<Logpointer> candidates;
    std::ranges::transform(
        std::move(snapshots), std::back_inserter(candidates), [](auto & obj) { return parseFromString<Logpointer>(obj.relative_path); });
    std::ranges::sort(candidates);

    if (std::ranges::count(candidates, end) > 0)
    {
        LOG_INFO(log, "Found leftover from previous GC run, discarding this batch");
        return end;
    }

    // Zookeeper lost without backups thus sequential counter reset but we have snapshot, first GC run
    if (const Logpointer greatest_end = *candidates.rbegin(); start == 0 && greatest_end > 0)
    {
        LOG_INFO(log, "batch start = 0 but found snapshot with {}, using it", greatest_end);
        return greatest_end;
    }

    return start;
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
    requests[log_batch_length] = zkutil::makeRemoveRequest(storage.traits.gc_lock_path, 0);

    storage.zookeeper()->multi(requests);
}

String ObjectStorageVFSGCThread::getNode(Logpointer ptr) const
{
    // Zookeeper's sequential node is 10 digits with padding zeros
    return fmt::format("{}{:010}", storage.traits.log_item, ptr);
}

StoredObject ObjectStorageVFSGCThread::getSnapshotObject(Logpointer ptr) const
{
    // We need a separate folder to quickly get snapshot with unknown logpointer on reconciliation
    return storage.getMetadataObject(fmt::format("{}/{}", SNAPSHOTS_PATH, ptr));
}
}
