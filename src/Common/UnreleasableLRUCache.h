#pragma once

#include <unordered_map>
#include <list>
#include <memory>
#include <chrono>
#include <mutex>
#include <atomic>

#include <Poco/Logger.h>
#include <base/logger_useful.h>


namespace DB
{

template <typename T>
struct TrivialUnreleasableLRUCacheWeightFunction
{
    size_t operator()(const T &) const
    {
        return 1;
    }
};

enum class CacheEvictStatus
{
    CAN_EVITCT, // a key can be evicted
    TERMINATE_EVICT, // stop the evicting process
    SKIP_EVICT, // skip current value and keep iterating
};

template <typename T>
struct TrivialUnreleasableLRUCacheEvitPolicy
{
    CacheEvictStatus canRelease(const T &)
    {
        return CacheEvictStatus::CAN_EVITCT;
    }

    void release(T & )
    {
    }
};

/*
 * Another version LRU Cache。
 * A value can only be evicted or be updated if it is releasable. If there is no releasable value,
 * insert or update will fail.
 */
template <typename TKey,
         typename TMapped,
         typename HashFunction = std::hash<TKey>,
         typename WeightFunction = TrivialUnreleasableLRUCacheWeightFunction<TMapped>,
         typename EvictPolicy = TrivialUnreleasableLRUCacheEvitPolicy<TMapped>>
class UnreleasableLRUCache
{
public:
    using Key = TKey;
    using Mapped = TMapped;
    using MappedPtr = std::shared_ptr<Mapped>;

    /** Initialize LRUCache with max_size and max_elements_size.
      * max_elements_size == 0 means no elements size restrictions.
      */
    UnreleasableLRUCache(size_t max_size_, size_t max_elements_size_ = 0)
        : max_size(std::max(static_cast<size_t>(1), max_size_))
        , max_elements_size(max_elements_size_)
        {}

    MappedPtr get(const Key & key)
    {
        std::lock_guard lock(mutex);

        auto res = getImpl(key, lock);
        if (res)
            ++hits;
        else
            ++misses;
        return res;
    }

    /*
     * Fail on two cases
     * 1) the key exists, but the old value is not releasable
     * 2) the key not exists, but there is not enough space for it after trying to evict some least recently used values.
     */
    bool set(const Key & key, const MappedPtr & mapped)
    {
        std::lock_guard lock(mutex);

        return setImpl(key, mapped, lock);
    }

    void getStats(size_t & out_hits, size_t & out_misses) const
    {
        std::lock_guard lock(mutex);
        out_hits = hits;
        out_misses = misses;
    }

    size_t weight() const
    {
        std::lock_guard lock(mutex);
        return current_size;
    }

    size_t count() const
    {
        std::lock_guard lock(mutex);
        return cells.size();
    }

    size_t maxSize() const
    {
        return max_size;
    }

    void reset()
    {
        std::lock_guard lock(mutex);
        queue.clear();
        cells.clear();
        current_size = 0;
        hits = 0;
        misses = 0;
    }

    virtual ~UnreleasableLRUCache() {}

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

    mutable std::mutex mutex;
private:

    LRUQueue queue;

    /// Total weight of values.
    size_t current_size = 0;
    const size_t max_size;
    const size_t max_elements_size;

    std::atomic<size_t> hits {0};
    std::atomic<size_t> misses {0};

    WeightFunction weight_function;
    EvictPolicy evict_policy;

    MappedPtr getImpl(const Key & key, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock)
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

    bool setImpl(const Key & key, const MappedPtr & mapped, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock)
    {
        auto [it, inserted] = cells.emplace(std::piecewise_construct,
            std::forward_as_tuple(key),
            std::forward_as_tuple());

        Cell & cell = it->second;

        if (inserted)
        {
            auto weight = mapped ? weight_function(*mapped) : 0;
            if (!removeOverflow(weight))
            {
                // cannot insert this new value
                cells.erase(it);
                return false;
            }

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
            if (evict_policy.canRelease(*cell.value) != CacheEvictStatus::CAN_EVITCT)
            {
                // the old value is not releasable
                return false;
            }
            evict_policy.release(*cell.value);
            current_size -= cell.size;
            queue.splice(queue.end(), queue, cell.queue_iterator);
        }

        cell.value = mapped;
        cell.size = cell.value ? weight_function(*cell.value) : 0;
        current_size += cell.size;

        removeOverflow(0);
        return true;
    }

    // Or make your own implementation
    virtual bool removeOverflow(size_t more_size)
    {
        size_t current_weight_lost = 0;
        size_t queue_size = cells.size();

        auto key_it = queue.begin();
        while ((current_size + more_size > max_size || (max_elements_size != 0 && queue_size > max_elements_size)) && (queue_size > 1)
                && key_it != queue.end())
        {
            const Key & key = *key_it;

            auto it = cells.find(key);
            if (it == cells.end())
            {
                LOG_ERROR(&Poco::Logger::get("UnreleasableLRUCache"), "UnreleasableLRUCache became inconsistent. There must be a bug in it.");
                abort();
            }

            const auto & cell = it->second;
            auto cache_evict_status = evict_policy.canRelease(*(cell.value));
            if (cache_evict_status == CacheEvictStatus::CAN_EVITCT)
            {
                evict_policy.release(*(cell.value));
                current_size -= cell.size;
                current_weight_lost += cell.size;

                cells.erase(it);
                key_it = queue.erase(key_it);
                --queue_size;
            }
            else if (cache_evict_status == CacheEvictStatus::SKIP_EVICT)
            {
                key_it++;
                continue;
            }
            else if (cache_evict_status == CacheEvictStatus::TERMINATE_EVICT)
            {
                break;
            }
        }

        onRemoveOverflowWeightLoss(current_weight_lost);

        if (current_size > (1ull << 63))
        {
            LOG_ERROR(&Poco::Logger::get("UnreleasableLRUCache"), "UnreleasableLRUCache became inconsistent. There must be a bug in it.");
            abort();
        }
        return !(current_size + more_size > max_size || (max_elements_size != 0 && queue_size > max_elements_size));
    }

    /// Override this method if you want to track how much weight was lost in removeOverflow method.
    virtual void onRemoveOverflowWeightLoss(size_t /*weight_loss*/) {}
};


}
