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
    /// replay it cancels the matching preceding DROP of the same block id, so a
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

    /// How many entries we have in log
    size_t entries_count{};
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

    bool insert(const std::string & key, const V & value)
    {
        auto it = map.find(key);
        if (it != map.end())
            return false;

        if (size() == max_size)
        {
            map.erase(queue.front().key);
            queue.pop_front();
        }

        ListNode elem{key, value};
        auto itr = queue.insert(queue.end(), elem);
        map.emplace(itr->key, itr);
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
    const bool disk_supports_writing_with_append;

    bool stopped{false};

    /// Start new log
    void rotate();

    /// Remove all old logs with non-needed records for deduplication_window
    void dropOutdatedLogs();

    /// Execute both previous methods if needed
    void rotateAndDropIfNeeded();

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
    /// Each rollback record - a CANCEL, or a DROP carrying
    /// DEDUPLICATION_LOG_CANCELLED_ADD_PART_NAME - cancels the matching
    /// preceding ADD or DROP (both are skipped), so an insert that failed and
    /// rolled back consumes no deduplication-window slot and a drop that failed
    /// and rolled back erases nothing; the remaining ADD/DROP records are
    /// applied exactly as they were live. Also recomputes each log's
    /// `entries_count` from only the surviving records (`record_log_numbers`
    /// maps each record back to its file), so retention does not count
    /// cancelled pairs as consumed deduplication-window slots and drop a log
    /// that still holds live block ids.
    void applyRecords(
        const std::vector<MergeTreeDeduplicationLogRecord> & records,
        const std::vector<size_t> & record_log_numbers);
};

}
