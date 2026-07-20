#pragma once
#include <base/StringViewHash.h>
#include <Core/Types.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Disks/IDisk.h>
#include <map>
#include <list>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace DB
{

/// Deduplication operation stored on disk: a part was added, dropped, or a
/// drop was rolled back.
enum class MergeTreeDeduplicationOp : uint8_t
{
    ADD = 1,
    DROP = 2,
    /// Written when a part drop that had already durably written some of its
    /// DROP records fails and must be rolled back (e.g. a write, rotation or
    /// fsync failure right after the writes): one CANCEL per written DROP. On
    /// replay it cancels the matching preceding DROP of the same block id and
    /// the same part name (block ids can be reused across part generations, so
    /// the part name pins the exact DROP this rollback undoes), so a
    /// rolled-back drop does not erase a block id that stayed published. The
    /// record carries the real part name, so a server from before this op
    /// existed replays it as the insert that restores the block id - the
    /// rollback's intended net effect (only the entry's position in the
    /// eviction order diverges) - keeping the log downgrade-safe without a
    /// format version. The rollback of a failed *insert* deliberately does not
    /// use this op: replaying an unknown op as an insert would keep the
    /// never-committed block id published on an older server, silently
    /// deduplicating - and dropping the data of - a client retry of the failed
    /// insert. It is encoded as a DROP record carrying
    /// DEDUPLICATION_LOG_CANCELLED_ADD_PART_NAME instead, which an older server
    /// replays as the erase that unpublishes the block id.
    CANCEL = 3,
};

/// The part name carried by a DROP record that rolls back the ADD record(s) of a
/// failed insert. It can never collide with a real record: real part names
/// always end in `_<min>_<max>_<level>`. The part name of a DROP record is never
/// parsed, on any server version, so an older server simply replays such a
/// record as a plain erase of the rolled-back block id - the correct net effect -
/// while servers with this code recognize the marker and cancel the (ADD, DROP)
/// pair out of the replay entirely, so the rolled-back insert does not consume a
/// deduplication-window slot either.
constexpr const char * DEDUPLICATION_LOG_CANCELLED_ADD_PART_NAME = "cancel";

/// Record for deduplication on disk
struct MergeTreeDeduplicationLogRecord
{
    MergeTreeDeduplicationOp operation{};
    std::string part_name;
    std::string block_id;
};

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
    /// a replay would actually apply to the in-memory map. Drives retention
    /// (`dropOutdatedLogs`): the cancelled pairs of a rolled-back operation
    /// contribute nothing, so they are not counted as consumed deduplication-window
    /// slots and cannot drop an older log that still holds committed block ids. The
    /// gap between this and `entries_count`, summed over all files, is the amount of
    /// unreclaimable rollback garbage that triggers `compact`.
    size_t effective_entries_count{};
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

    void setMaxSize(size_t max_size_)
    {
        max_size = max_size_;
        trimToMaxSize();
    }

    /// Evict the oldest entries (in FIFO insertion order) until the size is within
    /// the limit. Only pops entries, so it never allocates and never throws: a caller
    /// that has already made a change durable relies on this to enforce the window
    /// without a failure that could no longer be rolled back.
    void trimToMaxSize() noexcept
    {
        while (size() > max_size)
        {
            map.erase(queue.front().key);
            queue.pop_front();
        }
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
    std::vector<AddPartResult> addPart(const std::vector<std::string> & block_id, const MergeTreePartInfo & part);

    /// Remove all covered parts from in memory table and add DROP records to the disk
    void dropPart(const MergeTreePartInfo & drop_part_info);

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

    /// Writer to the current log file
    std::unique_ptr<WriteBufferFromFileBase> current_writer;

    /// Overall mutex because we can have a lot of concurrent inserts
    std::mutex state_mutex;

    /// Disk where log is stored
    DiskPtr disk;
    bool disk_supports_writing_with_append;

    bool stopped{false};

    /// Start new log
    void rotate();

    /// Remove all old logs with non-needed records for deduplication_window
    void dropOutdatedLogs();

    /// Remove the zero-record log files at the end of the history (see the definition).
    /// Without append support they would otherwise accumulate one per restart, because
    /// every rotation starts a fresh file and dropOutdatedLogs can only drop an oldest
    /// prefix. Called from load, before the first rotation.
    void removeTrailingEmptyLogs();

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
    void compact();

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
    void neutralizeOrphanLog(const std::string & path);

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
