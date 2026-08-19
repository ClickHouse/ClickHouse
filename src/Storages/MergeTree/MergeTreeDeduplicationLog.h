#pragma once
#include <Core/Types.h>
#include <base/StringRef.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Disks/IDisk.h>
#include <map>
#include <list>
#include <mutex>
#include <string>
#include <unordered_map>

namespace DB
{

/// Description of dedupliction log
struct MergeTreeDeduplicationLogNameDescription
{
    /// Path to log
    std::string path;

    /// How many entries we have in log
    size_t entries_count;
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
    using IndexMap = std::unordered_map<StringRef, typename Queue::iterator, StringRefHash>;

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
/// Also stores them in memory in hash table with limited size.
class MergeTreeDeduplicationLog
{
public:
    MergeTreeDeduplicationLog(
        const std::string & logs_dir_,
        size_t deduplication_window_,
        const MergeTreeDataFormatVersion & format_version_,
        DiskPtr disk_);

    /// Add part into in-memory hash table and to disk
    /// Return true and part info if insertion was successful.
    /// Otherwise, in case of duplicate, return false and previous part name with same hash (useful for logging)
    std::pair<MergeTreePartInfo, bool> addPart(const std::string & block_id, const MergeTreePartInfo & part);

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

    /// Overall mutex because we can have a lot of cocurrent inserts
    std::mutex state_mutex;

    /// Disk where log is stored
    DiskPtr disk;

    bool stopped{false};

    /// Start new log
    void rotate();

    /// Remove all old logs with non-needed records for deduplication_window
    void dropOutdatedLogs();

    /// Execute both previous methods if needed
    void rotateAndDropIfNeeded();

    /// Load single log from disk. In case of corruption throws exceptions
    size_t loadSingleLog(const std::string & path);
};

}
