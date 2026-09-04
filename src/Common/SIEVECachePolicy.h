#pragma once

#include <Common/ICachePolicy.h>
#include <Common/CurrentMetrics.h>

#include <list>
#include <unordered_map>

namespace DB
{

/// Cache policy SIEVE evicts entries using the SIEVE algorithm: a single "hand" walks through a FIFO
/// queue of entries and inspects each entry's `visited` flag. If the flag is set, it is cleared and the
/// hand advances; if the flag is clear, the entry is evicted. Newly inserted entries start with the flag
/// cleared and are appended to the back of the queue. This is a simple, scan-resistant alternative to LRU.
/// Also see cache policy LRU for reference.
/// WeightFunction is a functor that takes Mapped as a parameter and returns "weight" (approximate size) of that value.
/// Cache starts to evict entries when their total weight exceeds max_size_in_bytes.
/// Value weight should not change after insertion.
/// To work with the thread-safe implementation of this class use a class "CacheBase" with first parameter "SIEVE"
/// and next parameters in the same order as in the constructor of the current class.
/// For more details, see https://junchengyang.com/publication/nsdi24-SIEVE.pdf
template <typename Key, typename Mapped, typename HashFunction = std::hash<Key>, typename WeightFunction = EqualWeightFunction<Mapped>>
class SIEVECachePolicy : public ICachePolicy<Key, Mapped, HashFunction, WeightFunction>
{
public:
    using Base = ICachePolicy<Key, Mapped, HashFunction, WeightFunction>;
    using typename Base::MappedPtr;
    using typename Base::KeyMapped;
    using typename Base::OnRemoveEntryFunction;

    SIEVECachePolicy(
        CurrentMetrics::Metric size_in_bytes_metric_,
        CurrentMetrics::Metric count_metric_,
        size_t max_size_in_bytes_,
        size_t max_count_,
        OnRemoveEntryFunction on_remove_entry_function_)
        : Base(std::make_unique<NoCachePolicyUserQuota>())
        , max_size_in_bytes(max_size_in_bytes_)
        , max_count(max_count_)
        , current_size_in_bytes_metric(size_in_bytes_metric_)
        , count_metric(count_metric_)
        , on_remove_entry_function(on_remove_entry_function_)
    {
        hand = queue.begin();
    }

    ~SIEVECachePolicy() override
    {
        clearImpl();
    }

    size_t sizeInBytes() const override { return current_size_in_bytes; }

    size_t count() const override { return cells.size(); }

    size_t maxSizeInBytes() const override { return max_size_in_bytes; }

    size_t maxCount() const override { return max_count; }

    void setMaxCount(size_t max_count_) override
    {
        max_count = max_count_;
        removeOverflow(false);
    }

    void setMaxSizeInBytes(size_t max_size_in_bytes_) override
    {
        max_size_in_bytes = max_size_in_bytes_;
        removeOverflow(false);
    }

    void clear() override
    {
        clearImpl();
        hand = queue.begin();
    }

    void remove(const Key & key) override
    {
        auto it = cells.find(key);
        if (it == cells.end())
            return;

        auto & cell = it->second;
        current_size_in_bytes -= cell.size;
        CurrentMetrics::sub(current_size_in_bytes_metric, cell.size);

        const bool hand_points_to_removed = hand == cell.queue_iterator;
        auto next = queue.erase(cell.queue_iterator);
        if (hand_points_to_removed)
            hand = next == queue.end() ? queue.begin() : next;

        cells.erase(it);
        CurrentMetrics::sub(count_metric);
    }

    void remove(std::function<bool(const Key & key, const MappedPtr & mapped)> predicate) override
    {
        const size_t old_size_in_bytes = current_size_in_bytes;
        const size_t old_size = cells.size();

        for (auto it = cells.begin(); it != cells.end();)
        {
            if (predicate(it->first, it->second.value))
            {
                auto & cell = it->second;
                current_size_in_bytes -= cell.size;

                const bool hand_points_to_removed = hand == cell.queue_iterator;
                auto next = queue.erase(cell.queue_iterator);
                if (hand_points_to_removed)
                    hand = next == queue.end() ? queue.begin() : next;

                it = cells.erase(it);
            }
            else
                ++it;
        }

        CurrentMetrics::sub(current_size_in_bytes_metric, old_size_in_bytes - current_size_in_bytes);
        CurrentMetrics::sub(count_metric, old_size - cells.size());
    }

    MappedPtr get(const Key & key) override
    {
        auto it = cells.find(key);
        if (it == cells.end())
            return {};

        Cell & cell = it->second;
        cell.visited = true;

        return cell.value;
    }

    std::optional<KeyMapped> getWithKey(const Key & key) override
    {
        auto it = cells.find(key);
        if (it == cells.end())
            return std::nullopt;

        Cell & cell = it->second;
        cell.visited = true;

        return std::make_optional<KeyMapped>({it->first, cell.value});
    }

    bool contains(const Key & key) const override
    {
        return cells.contains(key);
    }

    void set(const Key & key, const MappedPtr & mapped) override
    {
        const size_t old_size_in_bytes = current_size_in_bytes;
        const size_t old_size = cells.size();

        auto [it, inserted] = cells.emplace(std::piecewise_construct,
            std::forward_as_tuple(key),
            std::forward_as_tuple());

        Cell & cell = it->second;

        if (inserted)
        {
            try
            {
                cell.queue_iterator = queue.insert(queue.end(), key);
            }
            catch (...)
            {
                cells.erase(it);
                throw;
            }
        }
        else
        {
            current_size_in_bytes -= cell.size;
            cell.visited = true;
        }

        cell.value = mapped;
        cell.size = cell.value ? weight_function(*cell.value) : 0;
        current_size_in_bytes += cell.size;

        CurrentMetrics::add(current_size_in_bytes_metric, static_cast<Int64>(current_size_in_bytes) - old_size_in_bytes);
        CurrentMetrics::add(count_metric, static_cast<Int64>(cells.size()) - old_size);

        /// Protect the just-inserted tail entry from immediate eviction only when a new entry was
        /// actually appended. On an update no new tail is created, so the hand must walk normally;
        /// otherwise an unrelated tail entry would wrongly receive the new-entry exemption.
        removeOverflow(/*ignore_last_element=*/ inserted);
    }

    std::vector<KeyMapped> dump() const override
    {
        std::vector<KeyMapped> res;
        for (const auto & [key, cell] : cells)
            res.push_back({key, cell.value});
        return res;
    }

    /// Returns the keys in their queue order. For unit tests.
    std::vector<Key> dumpQueue() const
    {
        std::vector<Key> res;
        for (const auto & key : queue)
            res.push_back(key);
        return res;
    }

    /// Returns the `visited` flag of an entry, or std::nullopt if the key is absent. For unit tests.
    std::optional<bool> isVisited(const Key & key) const
    {
        auto it = cells.find(key);
        if (it == cells.end())
            return std::nullopt;
        const Cell & cell = it->second;
        return cell.visited;
    }

    /// Returns the key the hand currently points to, or std::nullopt if the queue is empty. For unit tests.
    std::optional<Key> getHand() const
    {
        if (hand == queue.end())
            return std::nullopt;
        return *hand;
    }

private:
    using SIEVEQueue = std::list<Key>;
    using SIEVEQueueIterator = typename SIEVEQueue::iterator;

    struct Cell
    {
        MappedPtr value;
        size_t size = 0;
        bool visited = false;
        SIEVEQueueIterator queue_iterator;
    };
    using Cells = std::unordered_map<Key, Cell, HashFunction>;

    SIEVEQueue queue;
    SIEVEQueueIterator hand;
    Cells cells;

    /// Total weight of values.
    size_t current_size_in_bytes = 0;
    size_t max_size_in_bytes;
    size_t max_count;

    CurrentMetrics::Metric current_size_in_bytes_metric;
    CurrentMetrics::Metric count_metric;

    WeightFunction weight_function;
    OnRemoveEntryFunction on_remove_entry_function;

    void removeOverflow(bool ignore_last_element)
    {
        /// SIEVE algorithm:
        /// `hand` walks through the queue.
        /// If it points to a cell with visited == true, it sets visited = false and advances.
        /// If it points to a cell with visited == false, it evicts this cell.

        const size_t old_size_in_bytes = current_size_in_bytes;
        const size_t old_size = cells.size();

        while (!queue.empty()
               && (current_size_in_bytes > max_size_in_bytes || (max_count != 0 && queue.size() > max_count)))
        {
            if (hand == queue.end()
                || (ignore_last_element && queue.size() > 1 && hand == std::prev(queue.end())))
            {
                /// Do not evict the just-inserted element and wrap the hand back to the start.
                hand = queue.begin();
            }

            const Key & key = *hand;
            auto it = cells.find(key);
            if (it == cells.end())
                std::terminate(); // Queue became inconsistent

            auto & cell = it->second;

            if (!cell.visited)
            {
                current_size_in_bytes -= cell.size;
                if (on_remove_entry_function)
                    on_remove_entry_function(cell.size, cell.value);

                hand = queue.erase(hand);
                cells.erase(it);
            }
            else
            {
                cell.visited = false;
                ++hand;
            }
        }

        if (hand == queue.end())
            hand = queue.begin();

        CurrentMetrics::sub(current_size_in_bytes_metric, old_size_in_bytes - current_size_in_bytes);
        CurrentMetrics::sub(count_metric, old_size - cells.size());

        if (current_size_in_bytes > (1ull << 63))
            std::terminate(); // Queue became inconsistent
    }

    void clearImpl()
    {
        CurrentMetrics::sub(count_metric, cells.size());
        CurrentMetrics::sub(current_size_in_bytes_metric, current_size_in_bytes);

        queue.clear();
        cells.clear();
        current_size_in_bytes = 0;
    }
};

}
