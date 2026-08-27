#pragma once
#include <base/StringViewHash.h>
#include <Core/Types.h>
#include <Storages/MergeTree/MergeTreeDeduplicationLogRecord.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Disks/IDisk.h>
#include <map>
#include <list>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

namespace DB
{

/// Description of dedupliction log
struct MergeTreeDeduplicationLogNameDescription
{
    /// Path to log
    std::string path;

    /// Total number of records physically stored in this log file, including
    /// rollback records and the records they cancel out. Drives log rotation
    /// (`rotateAndDropIfNeeded`): a burst of rolled-back operations still grows this
    /// count, so a single file keeps rotating and cannot grow without bound.
    size_t entries_count{};

    /// Number of records that survive rollback-pair elimination - i.e. the records
    /// a replay would actually apply to the in-memory map. The gap between this and
    /// `entries_count`, summed over all files, is the amount of unreclaimable
    /// rollback garbage that triggers `compact`.
    size_t effective_entries_count{};

    /// Number of live block IDs whose latest ADD record is in this log. Drives
    /// retention (`dropOutdatedLogs`): a surviving DROP does not contribute to the
    /// reconstructed map, so it must not make an older ADD-only log look redundant.
    size_t live_entries_count{};
};

/// Simple string-key HashTable with fixed size based on STL containers.
/// Preserves order using linked list and remove elements
/// on overflow in FIFO order.
template <typename V>
class LimitedOrderedHashMap
{
private:
    struct ListNode
    {
        std::string key;
        V value;
    };
    using Queue = std::list<ListNode>;
    using IndexMap = std::unordered_map<std::string_view, typename Queue::iterator, StringViewHash>;

    Queue queue;
    IndexMap map;
    size_t max_size;
public:
    using iterator = typename Queue::iterator;
    using const_iterator = typename Queue::const_iterator;
    using reverse_iterator = typename Queue::reverse_iterator;
    using const_reverse_iterator = typename Queue::const_reverse_iterator;

    explicit LimitedOrderedHashMap(size_t max_size_)
        : max_size(max_size_)
    {}

    bool contains(const std::string & key) const
    {
        return map.find(key) != map.end();
    }

    V get(const std::string & key) const
    {
        return map.at(key)->value;
    }

    size_t size() const
    {
        return queue.size();
    }

    template <typename Callback>
    void setMaxSize(size_t max_size_, Callback && on_evict)
    {
        max_size = max_size_;
        trimToMaxSize(std::forward<Callback>(on_evict));
    }

    void setMaxSize(size_t max_size_)
    {
        setMaxSize(max_size_, [] (const std::string &, const V &) {});
    }

    /// Change the limit without enforcing it. The caller enforces it separately, which
    /// is what lets entries of an insert that is published but not yet confirmed
    /// committed outlive a window change (see
    /// MergeTreeDeduplicationLog::enforceDeduplicationWindow).
    void setMaxSizeWithoutTrimming(size_t max_size_)
    {
        max_size = max_size_;
    }

    /// Evict the oldest entries (in FIFO insertion order) until at most `target_size`
    /// remain. Only pops entries, so it never allocates and never throws: a caller
    /// that has already made a change durable relies on this to enforce the window
    /// without a failure that could no longer be rolled back.
    template <typename Callback>
    void trimToSize(size_t target_size, Callback && on_evict) noexcept
    {
        while (size() > target_size)
        {
            on_evict(queue.front().key, queue.front().value);
            map.erase(queue.front().key);
            queue.pop_front();
        }
    }

    template <typename Callback>
    void trimToMaxSize(Callback && on_evict) noexcept
    {
        trimToSize(max_size, std::forward<Callback>(on_evict));
    }

    void trimToMaxSize() noexcept
    {
        trimToMaxSize([] (const std::string &, const V &) {});
    }

    /// Erase every entry whose key and value satisfy the predicate, keeping the insertion
    /// order of the rest. Only erases, so it never allocates and never throws - which
    /// is what makes it usable on a failure path that must not be able to fail itself
    /// (see MergeTreeDeduplicationLog::unpublishFailedPart, where collecting the keys
    /// into a vector first could throw and leave the entries published).
    /// Returns how many entries were erased.
    template <typename Predicate>
    size_t eraseIf(Predicate && predicate) noexcept
    {
        size_t erased = 0;
        for (auto it = queue.begin(); it != queue.end();)
        {
            if (predicate(it->key, it->value))
            {
                map.erase(it->key);
                it = queue.erase(it);
                ++erased;
            }
            else
            {
                ++it;
            }
        }
        return erased;
    }

    bool erase(const std::string & key)
    {
        auto it = map.find(key);
        if (it == map.end())
            return false;

        auto queue_itr = it->second;
        map.erase(it);
        queue.erase(queue_itr);

        return true;
    }

    /// Insert a new entry without enforcing the size limit. This is the only part of
    /// an insertion that allocates - and so can throw - but it is strongly
    /// exception-safe (on failure the map is left exactly as it was) and, crucially,
    /// it never evicts. A caller that must be able to undo the insert can therefore
    /// roll it back with `erase` alone, which never allocates (so it cannot throw) and
    /// never drops an unrelated entry. Call `trimToMaxSize` to enforce the limit once
    /// the insert is known to succeed. Returns false if the key is already present.
    bool insertWithoutEviction(const std::string & key, const V & value)
    {
        if (map.find(key) != map.end())
            return false;

        auto itr = queue.insert(queue.end(), ListNode{key, value});
        try
        {
            map.emplace(itr->key, itr);
        }
        catch (...)
        {
            queue.erase(itr);
            throw;
        }
        return true;
    }

    bool insert(const std::string & key, const V & value)
    {
        if (!insertWithoutEviction(key, value))
            return false;
        trimToMaxSize();
        return true;
    }

    void clear()
    {
        map.clear();
        queue.clear();
    }

    iterator begin() { return queue.begin(); }
    const_iterator begin() const { return queue.cbegin(); }
    iterator end() { return queue.end(); }
    const_iterator end() const { return queue.cend(); }

    reverse_iterator rbegin() { return queue.rbegin(); }
    const_reverse_iterator rbegin() const { return queue.crbegin(); }
    reverse_iterator rend() { return queue.rend(); }
    const_reverse_iterator rend() const { return queue.crend(); }
};

/// Fixed-size log for deduplication in non-replicated MergeTree.
/// Stores records on disk for zero-level parts in human-readable format:
///  operation   part_name       partition_id_check_sum
///  1           88_18_18_0      88_10619499460461868496_9553701830997749308
///  2           77_14_14_0      77_15147918179036854170_6725063583757244937
///  2           77_15_15_0      77_14977227047908934259_8047656067364802772
///  1           77_20_20_0      77_15147918179036854170_6725063583757244937
/// The operation is one of MergeTreeDeduplicationOp: 1 = ADD, 2 = DROP,
/// 3 = CANCEL (rolls back a preceding DROP of a failed part drop). A DROP whose
/// part name is DEDUPLICATION_LOG_CANCELLED_ADD_PART_NAME rolls back a
/// preceding ADD of a failed insert. Both rollback encodings replay with the
/// correct net effect on older servers that do not know them (see
/// MergeTreeDeduplicationOp).
/// Also stores them in memory in hash table with limited size.
class MergeTreeDeduplicationLog
{
public:
    MergeTreeDeduplicationLog(
        const std::string & logs_dir_,
        size_t deduplication_window_,
        const MergeTreeDataFormatVersion & format_version_,
        DiskPtr disk_);

    struct AddPartResult
    {
        MergeTreePartInfo part_info;
        std::string block_id;
    };
    /// Add part into in-memory hash table and to disk
    /// Return empty block_id and part info if insertion was successful.
    /// Otherwise, in case of duplicate, return block_id with the collision and previous part name with same hash (useful for logging)
    ///
    /// Passing `part_was_published` makes the publication two-phase: the caller is told
    /// whether the block ids became published and takes responsibility for reporting the
    /// outcome of its own commit step with `finishPartPublication`, on every exit path.
    /// Until it does, those block ids are exempt from the deduplication window. A caller
    /// that passes nullptr has no commit step to roll back, so the window is enforced
    /// right away, as before.
    std::vector<AddPartResult> addPart(
        const std::vector<std::string> & block_id,
        const MergeTreePartInfo & part,
        bool * part_was_published = nullptr);

    /// Remove all covered parts from in memory table and add DROP records to the disk
    void dropPart(const MergeTreePartInfo & drop_part_info);

    /// Roll back a publication made by `addPart` for a part that never became active
    /// (see MergeTreeSink::commitPart). This is not a `dropPart`: the block ids were
    /// never committed, so the rollback is written with the reserved cancelled-add
    /// encoding, which cancels the (ADD, rollback) pair out of a replay entirely
    /// instead of replaying the transient ADD - which, with a full deduplication
    /// window, would evict an unrelated, already committed block id on the next start.
    /// Leaving the block ids published would silently deduplicate - and drop - a client
    /// retry of that insert, so this cannot fail: if the rollback records cannot be
    /// written, the block ids are unpublished anyway and the on-disk history, which no
    /// longer replays to the live state, is repaired or fenced off
    /// (see fenceOffDivergedHistory). Never throws.
    void rollbackPublishedPart(const MergeTreePartInfo & part_info, const std::vector<std::string> & block_ids) noexcept;

    /// Unpublish the block ids of a part that never became active without recording
    /// anything: the on-disk history, which still holds their ADD records, is repaired
    /// from the live state or fenced off instead. `rollbackPublishedPart` falls back to
    /// this when it cannot write; on its own it is the primitive a caller uses when it
    /// has no usable log to write to. Never throws.
    void unpublishFailedPart(const MergeTreePartInfo & part_info, const std::vector<std::string> * block_ids = nullptr) noexcept;

    /// Tell the log that the caller has finished with a two-phase publication made by
    /// `addPart`: the part either became active or was rolled back (with
    /// `rollbackPublishedPart`). Must be called exactly once for every `addPart` that
    /// reported a publication through `part_was_published`, on every exit path -
    /// `MergeTreeSink::commitPart` uses a scope guard.
    ///
    /// Until then the published block ids are exempt from the deduplication window:
    /// `addPart` cannot enforce it, because it returns before its caller knows whether
    /// the part will become active, and evicting the oldest entry there is not
    /// something the rollback could undo (restoring an evicted entry allocates, so it
    /// can fail on exactly the path that must not). With a full window that eviction
    /// would drop an unrelated, already committed block id whenever an insert failed to
    /// commit, and a retry of that unrelated insert would then be wrongly accepted and
    /// duplicate its data. Keeping the map overfull by the in-flight block ids instead
    /// makes the rollback a plain `erase` that restores the previous state exactly, and
    /// the window is enforced here, once the outcome is known.
    void finishPartPublication(const std::vector<std::string> & block_ids) noexcept;

    /// Load history from disk. Ignores broken logs.
    void load();

    void setDeduplicationWindowSize(size_t deduplication_window_);

    void shutdown();

    ~MergeTreeDeduplicationLog();

    /// For unit tests only. A disk without append support (e.g. `s3_plain_rewritable`)
    /// takes a different code path - every operation rotates into a fresh file and
    /// compaction reopens no file - but cannot be constructed cheaply in a unit test, so
    /// simulate its regime on a local disk. Must be called right after construction,
    /// before `load`.
    void simulateDiskWithoutWritingWithAppendSupportForTests() { disk_supports_writing_with_append = false; }
private:
    const std::string logs_dir;
    /// Size of deduplication window
    size_t deduplication_window;

    /// How often we create new logs. Not very important,
    /// default value equals deduplication_window * 2
    size_t rotate_interval;
    const MergeTreeDataFormatVersion format_version;

    /// Current log number. Always growing number.
    size_t current_log_number = 0;

    /// All existing logs in order of their numbers
    std::map<size_t, MergeTreeDeduplicationLogNameDescription> existing_logs;

    /// In memory hash-table
    LimitedOrderedHashMap<MergeTreePartInfo> deduplication_map;

    /// Log file containing the ADD record that currently publishes each block ID.
    /// It lets retention keep exactly the prefix of history required to reconstruct
    /// the live deduplication map.
    std::unordered_map<std::string, size_t> block_id_log_numbers;

    /// Writer to the current log file
    std::unique_ptr<WriteBufferFromFileBase> current_writer;

    /// Overall mutex because we can have a lot of concurrent inserts
    std::mutex state_mutex;

    /// Disk where log is stored
    DiskPtr disk;
    bool disk_supports_writing_with_append;

    /// Files left behind by a failed compaction that could neither be removed nor
    /// overwritten with an empty file (see neutralizeOrphanLog). While any is pending,
    /// the on-disk history is known to replay incorrectly on a restart, so operations
    /// fail closed until a retry (in prepareToWrite) neutralizes them all.
    std::set<std::string> orphan_logs_pending_neutralization;

    /// The on-disk unfinished-compaction marker (see markUnfinishedCompaction) is still
    /// active but the attempt to clear it failed. While it is set, prepareToWrite keeps
    /// retrying and fails operations closed: any record written now would be discarded
    /// by the restart the still-active marker triggers.
    bool compaction_marker_pending_clear = false;

    /// The on-disk history is known to have diverged from the live deduplication state
    /// (a rollback record could not be persisted), but the marker that would make the
    /// next restart discard that history could not be written either. Until it can,
    /// operations fail closed in prepareToWrite: a restart right now would replay
    /// records of an operation that never took effect and silently deduplicate its
    /// retry away.
    bool history_fence_pending = false;

    /// The marker is armed because the on-disk history diverged from the live state, not
    /// because a compaction is in flight. It must stay armed until the history is
    /// rewritten from the live state (a successful `compact`), so the paths that clear
    /// the marker once a failed compaction is fully rolled back must not clear this one.
    bool history_diverged = false;

    bool stopped{false};

    /// Number of block ids published by `addPart` whose caller has not yet reported the
    /// outcome of the part commit (see finishPartPublication). They are the newest
    /// entries of `deduplication_map` and are exempt from the deduplication window
    /// until then.
    size_t published_not_confirmed = 0;

    /// Erase the block ids of `part_info` (restricted to `block_ids` when given) from
    /// the in-memory map and its retention bookkeeping, without touching the log. Only
    /// erases, so it never allocates and never throws. Returns how many were erased.
    size_t unpublishBlockIds(const MergeTreePartInfo & part_info, const std::vector<std::string> * block_ids) noexcept;

    /// Enforce the deduplication window on the in-memory map, exempting the block ids
    /// of the publications that `addPart` has made but their caller has not resolved
    /// yet (see finishPartPublication). Only pops the oldest entries, so it never
    /// allocates and never throws.
    void enforceDeduplicationWindow() noexcept;

    /// Start new log
    void rotate();

    /// Remove all old logs with non-needed records for deduplication_window
    void dropOutdatedLogs();

    /// Ignore zero-record log files in the history and remove them best effort (see the
    /// definition). A compaction can leave such neutralized files behind when the disk
    /// rejects unlinking them; they must not make a later rotation retry that unlink.
    void removeEmptyLogs();

    /// Execute both previous methods if needed
    void rotateAndDropIfNeeded();

    /// Rewrite the whole live deduplication state into a single fresh log file and
    /// drop every older file. A rolled-back operation leaves an (ADD, rollback) or
    /// (DROP, CANCEL) record pair that cancels out on replay but holds no live state,
    /// and dropOutdatedLogs cannot reclaim it (the rollback record cancels a record in
    /// an older file that is still retained for other, live block ids, and retention
    /// only drops an oldest prefix). Rewriting the in-memory map - which already holds
    /// exactly the surviving state - as an ADD-per-entry snapshot discards all that
    /// accumulated garbage while reconstructing the identical state on the next replay.
    /// Returns whether the snapshot became the whole retained history; on failure
    /// nothing changed except that the failure barrier may now be armed (see
    /// markUnfinishedCompaction and neutralizeOrphanLog). Never throws.
    bool compact();

    /// Restore the invariant that the on-disk history replays to the live deduplication
    /// state, after that invariant has been broken by a rollback whose compensating
    /// records could not be persisted: the failed operation's ADD (or DROP) records
    /// stay durably on disk while the live map no longer reflects them, so a restart
    /// would replay an operation that never took effect - wrongly deduplicating the
    /// retry of a failed insert (silently dropping its data) or wrongly forgetting a
    /// block id whose drop failed.
    ///
    /// Repair is attempted first: `compact` rewrites the live map - which is correct
    /// and authoritative - as a fresh snapshot and drops every older file, so the
    /// abandoned records are gone and the history is consistent again. Only if that is
    /// impossible is the history fenced off instead, by arming the marker that makes
    /// the next `load` discard all of it (see markUnfinishedCompaction): a discarded
    /// window of deduplication history can at worst let a retry through as a visible
    /// duplicate, while replaying a diverged history drops data silently. If even the
    /// marker cannot be written, `history_fence_pending` keeps every later operation
    /// failing closed until it can. Never throws.
    void fenceOffDivergedHistory() noexcept;

    /// Remove a log file left behind by a failed compaction, or - if it cannot be
    /// removed - overwrite it with an empty file. A compaction snapshot is written at a
    /// log number one past `current_log_number`, so if the snapshot is made durable but
    /// the compaction then fails before switching over to it, the server keeps appending
    /// to the older, lower-numbered file while a stale snapshot survives at a HIGHER
    /// number. On the next restart `load` replays that snapshot last - after the older
    /// files that by then hold newer committed block ids - so its stale ADD records would
    /// resurrect evicted block ids and forget committed ones. Simply removing the file is
    /// therefore not optional; if the removal fails too, overwriting it with an empty
    /// file makes it replay as a no-op no matter where it sits, which preserves a
    /// consistent state instead of leaving a stale higher-numbered trap.
    /// Returns whether the file is no longer a hazard (removed or emptied). When both
    /// attempts fail the caller must not carry on as if the history were consistent:
    /// compact records the path in `orphan_logs_pending_neutralization`, and every
    /// subsequent operation retries the neutralization - refusing to write new records
    /// until it succeeds - in prepareToWrite.
    bool neutralizeOrphanLog(const std::string & path);

    /// Durably persist, before compact touches anything, the fact that a compaction is
    /// in flight, so its failure barrier survives a restart. neutralizeOrphanLog and
    /// `orphan_logs_pending_neutralization` protect only the current process: if the
    /// server restarts (or shuts down cleanly) while a file a failed compaction left
    /// behind is still stale, a new process knows nothing about it and `load` would
    /// replay the stale records as ordinary history - resurrecting evicted block ids or
    /// reordering the eviction order, either of which can silently deduplicate wrongly
    /// and drop the data of a later insert. The marker makes `load` discard the whole
    /// on-disk history instead (see discardHistoryAfterUnfinishedCompaction): losing at
    /// most one deduplication window of best-effort insert deduplication (a client
    /// retry may be accepted again, producing a visible duplicate) is strictly safer
    /// than replaying history that is known to be possibly inconsistent.
    ///
    /// The marker is the log file with number 0 - a number no real log can have, since
    /// rotation and compaction only ever create `current_log_number + 1` and numbers
    /// start at 1 - holding a single no-op rollback record. Servers with this code
    /// never replay it (load skips log number 0) and treat a non-empty marker as "the
    /// last compaction did not finish cleanly". The marker is cleared by removing the
    /// file or - if the removal fails - overwriting it with an empty file, so a marker
    /// that merely exists but is empty is inactive.
    ///
    /// The marker only protects servers that know it. A server from before the marker
    /// existed replays it as an ordinary log whose one record does nothing and carries
    /// on to the rest of the history, so it is NOT what keeps a downgrade safe. That is
    /// instead two invariants of compact itself: (1) while any stale file a failed
    /// compaction left behind is still readable, no new record is written anywhere -
    /// the failure path immediately removes such a file or truncates it to an empty
    /// one, which replays as a no-op on every server version, and until that
    /// neutralization succeeds every operation fails closed in prepareToWrite - so no
    /// server, however old, can replay a stale snapshot on top of records committed
    /// after it; and (2) a snapshot that survives a crash intact holds exactly the
    /// live state that replaying the older files reconstructs, so replaying it after
    /// them changes nothing. The residual exposure is a crash while some stale file
    /// could be neither removed nor emptied (the disk failed both a remove and a
    /// rewrite): a later downgraded server replays that file where a server with this
    /// code discards the whole history, and the replay can rebuild a wrong eviction
    /// order or, at worst, resurrect a block id whose part was since dropped -
    /// wrongly deduplicating its re-insert. A format-version gate was considered and
    /// deliberately not used: it cannot cover that corner anyway (the lingering files
    /// are written in the old format before the failure), while encoding the snapshot
    /// itself in a format older servers refuse would cost EVERY downgrade after any
    /// successful compaction the entire deduplication history - the snapshot is the
    /// permanent history from then on - a strictly larger regression than a
    /// double-fault-plus-crash window.
    /// Returns whether the marker is durably on disk; on failure the compaction must
    /// not start (a half-done compaction without the marker is the unprotected state
    /// this exists to prevent).
    bool markUnfinishedCompaction();

    /// Write the marker file itself, without any of the compaction-specific handling of
    /// a failure. The marker means "the files on disk may not replay to the state this
    /// server had", which an unfinished compaction is only one way to reach: a rollback
    /// whose compensating records could not be persisted leaves the same kind of
    /// divergence and arms the same marker (see fenceOffDivergedHistory).
    /// Returns whether the marker is durably on disk.
    bool writeHistoryDiscardMarker();

    /// Clear the unfinished-compaction marker (remove the file, or overwrite it with an
    /// empty one if the removal fails). Called once the on-disk history is consistent
    /// again: the compaction finished cleanly, its failure was fully rolled back, or
    /// every stale file it left behind has been neutralized. On failure sets
    /// `compaction_marker_pending_clear` so prepareToWrite retries and fails operations
    /// closed until then. Returns whether the marker is no longer active.
    bool clearCompactionMarker();

    /// Called from load, right after the directory scan: if the unfinished-compaction
    /// marker is active, the previous process died - or shut down - between starting a
    /// compaction and bringing the on-disk history back to a provably consistent state,
    /// so the files on disk may replay to a wrong deduplication state. Discard them all
    /// (remove, or overwrite with an empty file), clear the marker, and start with an
    /// empty history; throws if a file or the marker can neither be removed nor
    /// emptied, failing the load closed rather than replaying suspect history.
    /// Also called from setDeduplicationWindowSize when deduplication is re-enabled
    /// after having been disabled: a load with a window of zero skips this on purpose
    /// (nothing is replayed or written then), so the fenced-off files are still on disk,
    /// and appending new records to them would make the next restart discard the new
    /// records together with the stale ones. Discards a live `current_writer` along
    /// with the file it appends to and re-points `current_log_number` at the newest
    /// surviving file, so the caller can rotate to a fresh one.
    /// Precondition: `orphan_logs_pending_neutralization` is empty. Only the files
    /// registered in `existing_logs` are discarded here, so a stale file tracked only
    /// in the pending set would survive with its content while the marker below is
    /// cleared. Both callers guarantee it: load runs in a fresh process (the set is
    /// process-local), and setDeduplicationWindowSize drains the set through
    /// prepareToWrite before calling this.
    /// Throws without discarding anything when `deduplication_map` is not empty: the
    /// discard wipes `block_id_log_numbers`, so a block id published by an in-flight
    /// insert would survive only in the map, out of sync with every other structure.
    void discardHistoryAfterUnfinishedCompaction();

    /// Bring the log back to a writable, consistent state before an operation writes
    /// new records; called at the start of addPart and dropPart, before anything is
    /// written, so a throw here fails the operation cleanly and it can be retried.
    /// Also called from setDeduplicationWindowSize - the one path that can rotate the
    /// log or reopen a writer without an addPart or dropPart in front - before it
    /// rotates, clears the marker, or discards history, for the same reason.
    /// Three kinds of damage from an earlier failure are repaired:
    /// - An on-disk history that diverged from the live state because a rollback record
    ///   could not be persisted, and that could then not be fenced off either (see
    ///   fenceOffDivergedHistory). Retry arming the marker; if it still fails, throw -
    ///   fail closed - because a restart would replay an operation that never took
    ///   effect and silently deduplicate its retry away.
    /// - A file left behind by a failed compaction that could neither be removed nor
    ///   emptied (see neutralizeOrphanLog). Retry the neutralization; if it still
    ///   fails, throw - fail closed - because appending to a lower-numbered file while
    ///   a stale higher-numbered file survives would corrupt the next replay, silently
    ///   deduplicating wrongly after a restart.
    /// - A canceled `current_writer`. A failed write cancels the buffer, and the
    ///   rollback of the failed operation normally rotates to a fresh writer - but that
    ///   rotation can itself fail (e.g. the disk is still down), leaving the canceled
    ///   writer in place. Writes to a canceled buffer throw, so without healing it here
    ///   the first retry after the disk recovers would still fail (its own rollback
    ///   only then rotating), breaking the retryability contract for the double-fault
    ///   case. Rotating to a fresh writer up front makes the first retry succeed.
    void prepareToWrite();

    /// Compact when the raw record count has grown well beyond the effective coverage,
    /// i.e. when repeated rolled-back operations have left enough cancelled record pairs
    /// that the retained files (and the records load must replay) would otherwise keep
    /// growing without bound. A no-op in normal operation, where the two counts are
    /// equal.
    void compactIfNeeded();

    /// Read all records of a single log from disk in order, appending them to
    /// `records` (and the log's number, in lockstep, to `record_log_numbers`, so
    /// each record can be attributed back to its file). In case of corruption
    /// throws exceptions.
    void loadSingleLog(
        const std::string & path,
        size_t log_number,
        std::vector<MergeTreeDeduplicationLogRecord> & records,
        std::vector<size_t> & record_log_numbers);

    /// Replay a chronologically ordered record stream into the in-memory map.
    /// Each rollback record cancels the matching preceding record OF THE EXACT
    /// GENERATION IT UNDOES (both are skipped): a DROP carrying
    /// DEDUPLICATION_LOG_CANCELLED_ADD_PART_NAME undoes a preceding ADD of the
    /// same block id, a CANCEL undoes a preceding real DROP of the same block id
    /// and the same part name. Matching precisely keeps a rollback record whose
    /// target never reached disk from latching onto an older committed record -
    /// of the other kind, or of an earlier part generation that reused the same
    /// block id. An insert that failed and
    /// rolled back consumes no deduplication-window slot and a drop that failed
    /// and rolled back erases nothing; the remaining ADD/DROP records are
    /// applied exactly as they were live. Also recomputes each log's counts
    /// (`record_log_numbers` maps each record back to its file): the raw
    /// `entries_count` from every record (so rotation still accounts for the
    /// physical growth of rollback-heavy logs) and `effective_entries_count`
    /// from only the surviving records (so retention does not count cancelled
    /// pairs as consumed deduplication-window slots and drop a log that still
    /// holds live block ids).
    void applyRecords(
        const std::vector<MergeTreeDeduplicationLogRecord> & records,
        const std::vector<size_t> & record_log_numbers);
};

}
