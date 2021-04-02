#pragma once
#include <Core/Types.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>

namespace DB
{

struct MergeTreeDeduplicationLogNameDescription
{
    std::string path;
    size_t entries_count;
};


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
    const size_t max_size;
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

class MergeTreeDeduplicationLog
{
public:
    MergeTreeDeduplicationLog(
        const std::string & logs_dir_,
        size_t deduplication_window_,
        const MergeTreeDataFormatVersion & format_version_);

    std::pair<MergeTreePartInfo, bool> addPart(const std::string block_id, const MergeTreeData::MutableDataPartPtr & part);
    std::pair<MergeTreePartInfo, bool> addPart(const MergeTreeData::MutableDataPartPtr & part);
    void dropPart(const MergeTreeData::DataPartPtr & part);
    void dropPartition(const std::string & partition_id);

    void load();

private:
    const std::string logs_dir;
    const size_t deduplication_window;
    const size_t rotate_interval;
    const MergeTreeDataFormatVersion format_version;

    size_t current_log_number = 0;
    std::map<size_t, MergeTreeDeduplicationLogNameDescription> existing_logs;
    LimitedOrderedHashMap<MergeTreePartInfo> deduplication_map;
    std::unique_ptr<WriteBufferFromFile> current_writer;

    std::mutex state_mutex;

    void rotate();
    void dropOutdatedLogs();
    void rotateAndDropIfNeeded();
    size_t loadSingleLog(const std::string & path);
};

}
