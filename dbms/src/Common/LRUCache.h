#pragma once

#include <unordered_map>
#include <list>
#include <memory>
#include <chrono>
#include <mutex>
#include <atomic>

#include <common/logger_useful.h>


namespace DB
{

template <typename T>
struct TrivialWeightFunction
{
    size_t operator()(const T &) const
    {
        return 1;
    }
};


/// Thread-safe cache that evicts entries which are not used for a long time or are expired.
/// WeightFunction is a functor that takes Mapped as a parameter and returns "weight" (approximate size)
/// of that value.
/// Cache starts to evict entries when their total weight exceeds max_size and when expiration time of these
/// entries is due.
/// Value weight should not change after insertion.
template <typename TKey, typename TMapped, typename HashFunction = std::hash<TKey>, typename WeightFunction = TrivialWeightFunction<TMapped>>
class LRUCache
{
public:
    using Key = TKey;
    using Mapped = TMapped;
    using MappedPtr = std::shared_ptr<Mapped>;
    using Delay = std::chrono::seconds;

private:
    using Clock = std::chrono::steady_clock;
    using Timestamp = Clock::time_point;

public:
    LRUCache(size_t max_size_, const Delay & expiration_delay_ = Delay::zero())
        : max_size(std::max(static_cast<size_t>(1), max_size_)), expiration_delay(expiration_delay_) {}

    MappedPtr get(const Key & key)
    {
        std::lock_guard<std::mutex> lock(mutex);

        auto res = getImpl(key, lock);
        if (res)
            ++hits;
        else
            ++misses;

        return res;
    }

    void set(const Key & key, const MappedPtr & mapped)
    {
        std::lock_guard<std::mutex> lock(mutex);

        setImpl(key, mapped, lock);
    }

    /// If the value for the key is in the cache, returns it. If it is not, calls load_func() to
    /// produce it, saves the result in the cache and returns it.
    /// Only one of several concurrent threads calling getOrSet() will call load_func(),
    /// others will wait for that call to complete and will use its result (this helps prevent cache stampede).
    /// Exceptions occuring in load_func will be propagated to the caller. Another thread from the
    /// set of concurrent threads will then try to call its load_func etc.
    ///
    /// Returns std::pair of the cached value and a bool indicating whether the value was produced during this call.
    template <typename LoadFunc>
    std::pair<MappedPtr, bool> getOrSet(const Key & key, LoadFunc && load_func)
    {
        InsertTokenHolder token_holder;
        {
            std::lock_guard<std::mutex> cache_lock(mutex);

            auto val = getImpl(key, cache_lock);
            if (val)
            {
                ++hits;
                return std::make_pair(val, false);
            }

            auto & token = insert_tokens[key];
            if (!token)
                token = std::make_shared<InsertToken>(*this);

            token_holder.acquire(&key, token, cache_lock);
        }

        InsertToken * token = token_holder.token.get();

        std::lock_guard<std::mutex> token_lock(token->mutex);

        token_holder.cleaned_up = token->cleaned_up;

        if (token->value)
        {
            /// Another thread already produced the value while we waited for token->mutex.
            ++hits;
            return std::make_pair(token->value, false);
        }

        ++misses;
        token->value = load_func();

        std::lock_guard<std::mutex> cache_lock(mutex);

        /// Insert the new value only if the token is still in present in insert_tokens.
        /// (The token may be absent because of a concurrent reset() call).
        auto token_it = insert_tokens.find(key);
        if (token_it != insert_tokens.end() && token_it->second.get() == token)
            setImpl(key, token->value, cache_lock);

        if (!token->cleaned_up)
            token_holder.cleanup(token_lock, cache_lock);

        return std::make_pair(token->value, true);
    }

    void getStats(size_t & out_hits, size_t & out_misses) const
    {
        std::lock_guard<std::mutex> lock(mutex);
        out_hits = hits;
        out_misses = misses;
    }

    size_t weight() const
    {
        std::lock_guard<std::mutex> lock(mutex);
        return current_size;
    }

    size_t count() const
    {
        std::lock_guard<std::mutex> lock(mutex);
        return cells.size();
    }

    void reset()
    {
        std::lock_guard<std::mutex> lock(mutex);
        queue.clear();
        cells.clear();
        insert_tokens.clear();
        current_size = 0;
        hits = 0;
        misses = 0;
        current_weight_lost = 0;
    }

protected:
    /// Total weight of evicted values. This value is reset every time it is sent to profile events.
    size_t current_weight_lost = 0;

private:

    /// Represents pending insertion attempt.
    struct InsertToken
    {
        explicit InsertToken(LRUCache & cache_) : cache(cache_) {}

        std::mutex mutex;
        bool cleaned_up = false; /// Protected by the token mutex
        MappedPtr value; /// Protected by the token mutex

        LRUCache & cache;
        size_t refcount = 0; /// Protected by the cache mutex
    };

    using InsertTokenById = std::unordered_map<Key, std::shared_ptr<InsertToken>, HashFunction>;

    /// This class is responsible for removing used insert tokens from the insert_tokens map.
    /// Among several concurrent threads the first successful one is responsible for removal. But if they all
    /// fail, then the last one is responsible.
    struct InsertTokenHolder
    {
        const Key * key = nullptr;
        std::shared_ptr<InsertToken> token;
        bool cleaned_up = false;

        InsertTokenHolder() = default;

        void acquire(const Key * key_, const std::shared_ptr<InsertToken> & token_, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock)
        {
            key = key_;
            token = token_;
            ++token->refcount;
        }

        void cleanup([[maybe_unused]] std::lock_guard<std::mutex> & token_lock, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock)
        {
            token->cache.insert_tokens.erase(*key);
            token->cleaned_up = true;
            cleaned_up = true;
        }

        ~InsertTokenHolder()
        {
            if (!token)
                return;

            if (cleaned_up)
                return;

            std::lock_guard<std::mutex> token_lock(token->mutex);

            if (token->cleaned_up)
                return;

            std::lock_guard<std::mutex> cache_lock(token->cache.mutex);

            --token->refcount;
            if (token->refcount == 0)
                cleanup(token_lock, cache_lock);
        }
    };

    friend struct InsertTokenHolder;

    using LRUQueue = std::list<Key>;
    using LRUQueueIterator = typename LRUQueue::iterator;

    struct Cell
    {
        bool expired(const Timestamp & last_timestamp, const Delay & expiration_delay) const
        {
            return (expiration_delay == Delay::zero()) ||
                ((last_timestamp > timestamp) && ((last_timestamp - timestamp) > expiration_delay));
        }

        MappedPtr value;
        size_t size;
        LRUQueueIterator queue_iterator;
        Timestamp timestamp;
    };

    using Cells = std::unordered_map<Key, Cell, HashFunction>;

    InsertTokenById insert_tokens;

    LRUQueue queue;
    Cells cells;

    /// Total weight of values.
    size_t current_size = 0;
    const size_t max_size;
    const Delay expiration_delay;

    mutable std::mutex mutex;
    std::atomic<size_t> hits {0};
    std::atomic<size_t> misses {0};

    WeightFunction weight_function;

    MappedPtr getImpl(const Key & key, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock)
    {
        auto it = cells.find(key);
        if (it == cells.end())
        {
            return MappedPtr();
        }

        Cell & cell = it->second;
        updateCellTimestamp(cell);

        /// Move the key to the end of the queue. The iterator remains valid.
        queue.splice(queue.end(), queue, cell.queue_iterator);

        return cell.value;
    }

    void setImpl(const Key & key, const MappedPtr & mapped, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock)
    {
        auto res = cells.emplace(std::piecewise_construct,
            std::forward_as_tuple(key),
            std::forward_as_tuple());

        Cell & cell = res.first->second;
        bool inserted = res.second;

        if (inserted)
        {
            cell.queue_iterator = queue.insert(queue.end(), key);
        }
        else
        {
            current_size -= cell.size;
            queue.splice(queue.end(), queue, cell.queue_iterator);
        }

        cell.value = mapped;
        cell.size = cell.value ? weight_function(*cell.value) : 0;
        current_size += cell.size;
        updateCellTimestamp(cell);

        removeOverflow(cell.timestamp);
    }

    void updateCellTimestamp(Cell & cell)
    {
        if (expiration_delay != Delay::zero())
            cell.timestamp = Clock::now();
    }

    void removeOverflow(const Timestamp & last_timestamp)
    {
        size_t queue_size = cells.size();
        while ((current_size > max_size) && (queue_size > 1))
        {
            const Key & key = queue.front();

            auto it = cells.find(key);
            if (it == cells.end())
            {
                LOG_ERROR(&Logger::get("LRUCache"), "LRUCache became inconsistent. There must be a bug in it.");
                abort();
            }

            const auto & cell = it->second;
            if (!cell.expired(last_timestamp, expiration_delay))
                break;

            current_size -= cell.size;
            current_weight_lost += cell.size;

            cells.erase(it);
            queue.pop_front();
            --queue_size;
        }

        if (current_size > (1ull << 63))
        {
            LOG_ERROR(&Logger::get("LRUCache"), "LRUCache became inconsistent. There must be a bug in it.");
            abort();
        }
    }
};


}
