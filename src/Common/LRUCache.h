#pragma once

#include <unordered_map>
#include <list>
#include <memory>
#include <chrono>
#include <mutex>
#include <atomic>
#include <base/logger_useful.h>


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

template <typename T>
struct TrivialLRUCacheEvictPolicy
{
    inline bool canRelease(std::shared_ptr<T>) const
    {
        return true;
    }

    inline void release(std::shared_ptr<T>)
    {
    }
};


/// Thread-safe cache that evicts entries which are not used for a long time.
/// WeightFunction is a functor that takes Mapped as a parameter and returns "weight" (approximate size)
/// of that value.
/// Cache starts to evict entries when their total weight exceeds max_size.
/// Value weight should not change after insertion.
template <typename TKey,
         typename TMapped,
         typename HashFunction = std::hash<TKey>,
         typename WeightFunction = TrivialWeightFunction<TMapped>,
         typename EvictPolicy = TrivialLRUCacheEvictPolicy<TMapped>>
class LRUCache
{
public:
    using Key = TKey;
    using Mapped = TMapped;
    using MappedPtr = std::shared_ptr<Mapped>;
    struct Result
    {
        MappedPtr value;
        bool cache_miss = true;
        // set_successful is not trustworthy for getOrSet, because removeOverflow is called right after putting key in cache
        bool set_successful = false;
    };

    /** Initialize LRUCache with max_size and max_elements_size.
      * max_elements_size == 0 means no elements size restrictions.
      */
    LRUCache(size_t max_size_, size_t max_elements_size_ = 0)
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

    void set(const Key & key, const MappedPtr & mapped)
    {
        std::lock_guard lock(mutex);
        setImpl(key, mapped, lock);
    }

    /**
     * trySet() will fail (return false) if there is no space left and no keys could be evicted.
     * Eviction permission of each key is defined by EvictPolicy. In default policy there is no restriction.
     */
    bool trySet(const Key & key, const MappedPtr & mapped)
    {
        std::lock_guard lock(mutex);

        return setImpl(key, mapped, lock);
    }

    template <typename LoadFunc>
    std::pair<MappedPtr, bool> getOrSet(const Key & key, LoadFunc && load_func)
    {
        auto result = getOrTrySet(key, std::move(load_func));
        return std::make_pair(result.value, result.cache_miss);
    }

    void remove(const Key & key)
    {
        std::lock_guard lock(mutex);
        auto it = cells.find(key);
        if (it == cells.end())
            return;
        auto & cell = it->second;
        current_size -= cell.size;
        queue.erase(cell.queue_iterator);
        cells.erase(it);
    }

    /// If the value for the key is in the cache, returns it. If it is not, calls load_func() to
    /// produce it, saves the result in the cache and returns it.
    /// Only one of several concurrent threads calling getOrTrySet() will call load_func(),
    /// others will wait for that call to complete and will use its result (this helps prevent cache stampede).
    /// Exceptions occurring in load_func will be propagated to the caller. Another thread from the
    /// set of concurrent threads will then try to call its load_func etc.
    ///
    template <typename LoadFunc>
    Result getOrTrySet(const Key &key, LoadFunc && load_func)
    {
        InsertTokenHolder token_holder;
        {
            std::lock_guard cache_lock(mutex);

            auto val = getImpl(key, cache_lock);
            if (val)
            {
                ++hits;
                return {val, false, false};
            }

            auto & token = insert_tokens[key];
            if (!token)
                token = std::make_shared<InsertToken>(*this);

            token_holder.acquire(&key, token, cache_lock);
        }

        InsertToken * token = token_holder.token.get();

        std::lock_guard token_lock(token->mutex);

        token_holder.cleaned_up = token->cleaned_up;

        if (token->value)
        {
            /// Another thread already produced the value while we waited for token->mutex.
            ++hits;
            return {token->value, false, false};
        }

        ++misses;
        token->value = load_func();

        std::lock_guard cache_lock(mutex);

        /// Insert the new value only if the token is still in present in insert_tokens.
        /// (The token may be absent because of a concurrent reset() call).
        bool is_value_loaded = false;
        bool is_value_updated = false;
        auto token_it = insert_tokens.find(key);
        if (token_it != insert_tokens.end() && token_it->second.get() == token)
        {
            // setImpl() may fail, but the final behavior seems not be affected
            // next call of getOrTrySet() will still call load_func()
            is_value_updated = setImpl(key, token->value, cache_lock);
            is_value_loaded = true;
        }

        if (!token->cleaned_up)
            token_holder.cleanup(token_lock, cache_lock);

        return {token->value, is_value_loaded, is_value_updated};
    }

    /// If key is not in cache or the element can be released, return is true. otherwise, return is false
    bool tryRemove(const Key & key)
    {
        std::lock_guard loc(mutex);
        auto it = cells.find(key);
        if (it == cells.end())
            return true;
        auto & cell = it->second;
        if (!evict_policy.canRelease(cell.value))
            return false;
        evict_policy.release(cell.value);

        current_size -= cell.size;
        cells.erase(it);
        queue.erase(cell.queue_iterator);
        return true;
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
        insert_tokens.clear();
        current_size = 0;
        hits = 0;
        misses = 0;
    }

    virtual ~LRUCache() {}

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

            std::lock_guard token_lock(token->mutex);

            if (token->cleaned_up)
                return;

            std::lock_guard cache_lock(token->cache.mutex);

            --token->refcount;
            if (token->refcount == 0)
                cleanup(token_lock, cache_lock);
        }
    };

    friend struct InsertTokenHolder;


    InsertTokenById insert_tokens;

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
            auto value_weight = mapped ? weight_function(*mapped) : 0;
            if (!removeOverflow(value_weight))
            {
                // cannot find enough space to put in the new value
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
            if (!evict_policy.canRelease(cell.value))
            {
                // the old value is referred by someone, cannot release now
                // in default policy, it is always true.
                return false;
            }
            evict_policy.release(cell.value); // release the old value. this action is empty in default policy.
            current_size -= cell.size;
            queue.splice(queue.end(), queue, cell.queue_iterator);
        }

        cell.value = mapped;
        cell.size = cell.value ? weight_function(*cell.value) : 0;
        current_size += cell.size;

        return true;
    }

    bool removeOverflow(size_t required_size_to_remove = 0)
    {
        size_t current_weight_lost = 0;
        size_t queue_size = cells.size();
        auto key_it = queue.begin();
        auto is_overflow = [&] { return (current_size + required_size_to_remove > max_size || (max_elements_size != 0 && queue_size > max_elements_size)); };
        while (is_overflow() && (queue_size > 1) && (key_it != queue.end()))
        {
            const Key & key = *key_it;

            auto it = cells.find(key);
            if (it == cells.end())
            {
                LOG_ERROR(&Poco::Logger::get("LRUCache"), "LRUCache became inconsistent. There must be a bug in it.");
                abort();
            }

            const auto & cell = it->second;
            if (evict_policy.canRelease(cell.value))// in default, it is true
            {
                // always call release() before erasing an element
                // in default, it's an empty action
                evict_policy.release(cell.value);

                current_size -= cell.size;
                current_weight_lost += cell.size;

                cells.erase(it);
                key_it = queue.erase(key_it);
                --queue_size;
            }
            else
            {
                key_it++;
            }
        }

        onRemoveOverflowWeightLoss(current_weight_lost);

        if (current_size > (1ull << 63))
        {
            LOG_ERROR(&Poco::Logger::get("LRUCache"), "LRUCache became inconsistent. There must be a bug in it.");
            abort();
        }
        return !is_overflow();
    }

    /// Override this method if you want to track how much weight was lost in removeOverflow method.
    virtual void onRemoveOverflowWeightLoss(size_t /*weight_loss*/) {}
};


}
