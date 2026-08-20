#pragma once

#include <Core/Block.h>
#include <Disks/IDisk.h>
#include <Common/ThreadPool.h>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <unordered_set>
#include <vector>

namespace DB
{

enum class OverwriteCachePersistMode : uint8_t
{
    /// The table is in-memory only. Nothing is written and a restart produces an empty cache.
    None,
    /// A publication is queued for persistence and becomes visible immediately. A crash can lose the
    /// tail of the log, which a cache rebuilt from an upstream source can afford.
    Async,
    /// An `INSERT` returns only once its publication is durable.
    Sync,
};

OverwriteCachePersistMode parseOverwriteCachePersistMode(const String & value);
std::string_view toString(OverwriteCachePersistMode mode);

/** On-disk log that lets an `OverwriteCache` table survive a restart.
  *
  * Committed payloads already live in immutable row segments, so a segment is written to its own file
  * once, is never rewritten, and its file is deleted when the segment it mirrors is released. Nothing
  * else is written: the primary index, the lookup postings, the entry table and the version chains are
  * all rebuilt at load time, because every indexed column must belong to the `KEYS` tuple and is
  * therefore stored inside the segment itself.
  *
  * `manifest` holds one commit record per publication, in publication order: the segments the
  * publication added, the segments it retired, and the keys it deleted. Deletions have to be recorded
  * because a segment that a `DELETE` leaves partially dead is not superseded by any later segment, so
  * replaying that segment alone would resurrect the deleted keys.
  *
  * Replay skips the added segments that a later commit retired and applies the surviving ones in
  * publication order through ordinary winner selection. Every stored row was a winner when it was
  * published, so this reproduces the exact state, including the equal-version tie-break rule - which
  * resolves in favour of the row already stored and therefore depends on insertion order.
  */
class OverwriteCachePersistence
{
public:
    struct AddedSegment
    {
        UInt64 segment_id = 0;
        /// Segment columns as stored, which for `compress_segments` are compressed in memory. They are
        /// written out decompressed, so that the file does not depend on that setting.
        Columns columns;
        UInt64 rows = 0;
    };

    /// Everything one publication changed on disk. An insert adds a segment and retires the segments its
    /// replacements emptied; a delete records its keys and adds the segments compaction rewrote.
    struct Commit
    {
        UInt64 generation = 0;
        std::vector<AddedSegment> added;
        std::vector<UInt64> removed;
        std::vector<String> deleted_keys;
    };

    /// One replayed record. Either a segment block or a batch of deleted keys, never both: a commit is
    /// handed to the loader as its deletions followed by its segments.
    struct LoadedRecord
    {
        UInt64 segment_id = 0;
        Block block;
        std::vector<String> deleted_keys;
    };

    OverwriteCachePersistence(
        OverwriteCachePersistMode mode_,
        DiskPtr disk_,
        String path_,
        Block header_,
        String fingerprint_,
        String log_name_);

    ~OverwriteCachePersistence();

    bool isEnabled() const { return mode != OverwriteCachePersistMode::None; }

    /// Replays the log. `apply` runs on the loading thread in log order, before `start`, so replay
    /// itself writes nothing. A segment that replay leaves without a live row is retired by the caller
    /// through an ordinary commit once persistence has started.
    void load(const std::function<void(LoadedRecord &&)> & apply);

    /// Starts persisting. Called once the table is loaded, so that replay itself writes nothing.
    void start();

    UInt64 allocateSegmentId() { return next_segment_id.fetch_add(1, std::memory_order_relaxed); }

    /// Called with the storage's writer lock held, so that the log order is the publication order.
    /// Returns the sequence number to wait for in `Sync` mode.
    UInt64 enqueue(Commit && commit) TSA_NO_THREAD_SAFETY_ANALYSIS;

    /// Called without the writer lock, so a durable write never blocks another publication.
    void waitDurable(UInt64 sequence) TSA_NO_THREAD_SAFETY_ANALYSIS;

    /// Drops every segment and starts a fresh log. `TRUNCATE` already holds the table exclusively.
    void truncate();

    /// Removes the whole directory for `DROP TABLE`.
    void removeAllFiles();

    void rename(const String & new_path);

    void shutdown();

    /// Defers file removal for the duration of a `BACKUP`, so that the entries it collected stay
    /// readable while a concurrent publication retires their segments.
    class BackupPin
    {
    public:
        explicit BackupPin(OverwriteCachePersistence & persistence_);
        ~BackupPin();

    private:
        OverwriteCachePersistence & persistence;
    };

    /// Makes the log self-contained and returns the files a backup has to copy, the manifest first.
    /// Requires a live `BackupPin`.
    std::vector<String> collectFilesForBackup();

    /// Replaces the table's data with files taken from a backup. The table must be empty.
    void restoreFileFromBackup(const String & file_name, ReadBuffer & in);

    String getPath() const;
    DiskPtr getDisk() const { return disk; }
    static constexpr std::string_view manifest_file_name = "manifest";

private:
    struct Record
    {
        struct Segment
        {
            UInt64 segment_id = 0;
            UInt64 rows = 0;
        };

        UInt64 generation = 0;
        std::vector<Segment> added;
        std::vector<UInt64> removed;
        std::vector<String> deleted_keys;
    };

    void writerThread() TSA_NO_THREAD_SAFETY_ANALYSIS;
    void writeCommit(const Commit & commit);
    void writeSegmentFile(const AddedSegment & segment);
    void appendManifestRecord(const Record & record) TSA_REQUIRES(state_mutex);
    void openManifest(bool rewrite) TSA_REQUIRES(state_mutex);
    void closeManifest() TSA_REQUIRES(state_mutex);
    void createDirectories();
    void removeSegmentFiles(const std::vector<UInt64> & segment_ids);
    /// Collapses the added/retired bookkeeping of the log. Deletions are kept, because a surviving
    /// segment written before one still shadows the key it removed; only those that precede every
    /// surviving segment can be dropped.
    void checkpointManifest() TSA_REQUIRES(state_mutex);
    bool needsCheckpoint() const TSA_REQUIRES(state_mutex);
    std::vector<Record> readManifest(std::unordered_set<UInt64> & removed_segments) const;
    void writeManifest(const std::vector<Record> & records) TSA_REQUIRES(state_mutex);
    String segmentFileName(UInt64 segment_id) const;
    void setException();

    const OverwriteCachePersistMode mode;
    const DiskPtr disk;
    const Block header;
    const SharedHeader shared_header;
    const String fingerprint;
    const LoggerPtr log;

    mutable std::mutex path_mutex;
    String path TSA_GUARDED_BY(path_mutex);

    std::atomic<UInt64> next_segment_id{1};

    /// The log itself and the segments it references. Normally touched only by the writer thread, but a
    /// `BACKUP` collapses the log from its own thread, so it needs a lock of its own. Taken before
    /// `mutex` wherever both are needed.
    mutable std::mutex state_mutex;
    std::unique_ptr<WriteBufferFromFileBase> manifest_buffer TSA_GUARDED_BY(state_mutex);
    std::vector<Record::Segment> live_segments TSA_GUARDED_BY(state_mutex);
    std::unordered_set<UInt64> live_segment_ids TSA_GUARDED_BY(state_mutex);
    /// Records written since the last checkpoint, to decide when the bookkeeping is worth collapsing.
    size_t records_since_checkpoint TSA_GUARDED_BY(state_mutex) = 0;

    mutable std::mutex mutex;
    std::condition_variable queue_changed;
    std::condition_variable durable_changed;
    std::deque<std::pair<UInt64, Commit>> queue TSA_GUARDED_BY(mutex);
    UInt64 queued_bytes TSA_GUARDED_BY(mutex) = 0;
    UInt64 next_sequence TSA_GUARDED_BY(mutex) = 1;
    UInt64 durable_sequence TSA_GUARDED_BY(mutex) = 0;
    std::exception_ptr writer_exception TSA_GUARDED_BY(mutex);
    bool started TSA_GUARDED_BY(mutex) = false;
    bool stopped TSA_GUARDED_BY(mutex) = false;
    size_t backup_pins TSA_GUARDED_BY(mutex) = 0;
    std::vector<UInt64> deferred_removals TSA_GUARDED_BY(mutex);

    std::optional<ThreadFromGlobalPool> writer_thread;

    /// A publication may not outrun the disk without bound, or the queued payloads become a second copy
    /// of the table. Reaching the limit makes the next publication wait for the writer.
    static constexpr UInt64 max_queued_bytes = 256ULL * 1024 * 1024;
    static constexpr size_t checkpoint_min_records = 4096;
    static constexpr UInt8 format_version = 1;
};

using OverwriteCachePersistencePtr = std::shared_ptr<OverwriteCachePersistence>;

}
