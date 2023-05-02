#pragma once

#include <unordered_map>
#include <list>
#include <memory>
#include <chrono>
#include <mutex>
#include <atomic>

#include <Common/logger_useful.h>


namespace DB
{


/// Thread-safe cache that evicts entries which are not used for a long time.
/// WeightFunction is a functor that takes Mapped as a parameter and returns "weight" (approximate size)
/// of that value.
/// Cache starts to evict entries when their total weight exceeds max_size.
/// Value weight should not change after insertion.
template <typename TKey, typename TMapped, typename HashFunction = std::hash<TKey>>
class LRULockFree
{
public:
    using Key = TKey;
    using Mapped = TMapped;
    using MappedPtr = std::shared_ptr<Mapped>;

    /** Initialize LRUCache with max_size and max_elements_size.
      * max_elements_size == 0 means no elements size restrictions.
      */
    explicit LRULockFree(size_t max_size_, size_t max_elements_size_ = 0)
        : max_size(std::max(static_cast<size_t>(1), max_size_)), max_elements_size(max_elements_size_)
    {}

    MappedPtr get(const Key & key)
    {
        auto res = getImpl(key);
        if (res)
            ++hits;
        else
            ++misses;

        return res;
    }

    void set(const Key & key, const MappedPtr & mapped)
    {
        setImpl(key, mapped);
    }

    void remove(const Key & key)
    {
        auto it = cells.find(key);
        if (it == cells.end())
            return;
        auto & cell = it->second;
        current_size -= cell.size;
        queue.erase(cell.queue_iterator);
        cells.erase(it);
    }

    std::pair<MappedPtr, bool> removeWithReturn(const Key & key)
    {
        auto it = cells.find(key);
        if (it == cells.end())
        {
            return {nullptr, false};
        }
        auto & cell = it->second;
        auto res = std::make_pair(cell.value, true);

        current_size -= cell.size;
        queue.erase(cell.queue_iterator);
        cells.erase(it);
        return res;
    }

    void getStats(size_t & out_hits, size_t & out_misses) const
    {
        out_hits = hits;
        out_misses = misses;
    }

    size_t weight() const
    {
        return current_size;
    }

    size_t count() const
    {
        return cells.size();
    }

    size_t maxSize() const
    {
        return max_size;
    }

    void reset()
    {
        queue.clear();
        cells.clear();
        current_size = 0;
        hits = 0;
        misses = 0;
    }

    virtual ~LRULockFree() = default;

protected:
    using LRUQueue = std::list<Key>;
    using LRUQueueIterator = typename LRUQueue::iterator;

    struct Cell
    {
        MappedPtr value;
        size_t size;
        LRUQueueIterator queue_iterator;
    };

    using Cells = std::unordered_map<Key, Cell, HashFunction>;

    Cells cells;

private:

    LRUQueue queue;

    /// Total weight of values.
    size_t current_size = 0;
    const size_t max_size;
    const size_t max_elements_size;

    std::atomic<size_t> hits {0};
    std::atomic<size_t> misses {0};

    MappedPtr getImpl(const Key & key)
    {
        auto it = cells.find(key);
        if (it == cells.end())
        {
            return MappedPtr();
        }

        Cell & cell = it->second;

        /// Move the key to the end of the queue. The iterator remains valid.
        queue.splice(queue.end(), queue, cell.queue_iterator);

        return cell.value;
    }

    void setImpl(const Key & key, const MappedPtr & mapped)
    {
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
            current_size -= cell.size;
            queue.splice(queue.end(), queue, cell.queue_iterator);
        }

        cell.value = mapped;
        cell.size = cell.value ? 1 : 0;
        current_size += cell.size;

        removeOverflow();
    }

    void removeOverflow()
    {
        size_t current_weight_lost = 0;
        size_t queue_size = cells.size();

        while ((current_size > max_size || (max_elements_size != 0 && queue_size > max_elements_size)) && (queue_size > 1))
        {
            const Key & key = queue.front();

            auto it = cells.find(key);
            if (it == cells.end())
            {
                LOG_ERROR(&Poco::Logger::get("LRULockFree"), "LRUCache became inconsistent. There must be a bug in it.");
                abort();
            }

            const auto & cell = it->second;

            current_size -= cell.size;
            current_weight_lost += cell.size;

            cells.erase(it);
            queue.pop_front();
            --queue_size;
        }

        onRemoveOverflowWeightLoss(current_weight_lost);

        if (current_size > (1ull << 63))
        {
            LOG_ERROR(&Poco::Logger::get("LRULockFree"), "LRUCache became inconsistent. There must be a bug in it.");
            abort();
        }
    }

    /// Override this method if you want to track how much weight was lost in removeOverflow method.
    virtual void onRemoveOverflowWeightLoss(size_t /*weight_loss*/) {}
};


}
