#pragma once

#include <atomic>
#include <chrono>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <Common/logger_useful.h>

namespace DB
{
template <typename T>
struct TrivialLRUResourceCacheWeightFunction
{
    size_t operator()(const T &) const noexcept { return 1; }
};

template <typename T>
struct TrivialLRUResourceCacheReleaseFunction
{
    void operator()(std::shared_ptr<T>) noexcept { }
};

/**
 * Similar to implementation in LRUCachePolicy.h, but with the difference that keys can
 * only be evicted when they are releasable. Release state is controlled by this implementation.
 * get() and getOrSet() methods return a Holder to actual value, which does release() in destructor.
 *
 * Warning (!): This implementation is in development, not to be used.
 */
template <
    typename TKey,
    typename TMapped,
    typename WeightFunction = TrivialLRUResourceCacheWeightFunction<TMapped>,
    typename ReleaseFunction = TrivialLRUResourceCacheReleaseFunction<TMapped>,
    typename HashFunction = std::hash<TKey>>
class LRUResourceCache
{
public:
    using Key = TKey;
    using Mapped = TMapped;
    using MappedPtr = std::shared_ptr<Mapped>;

    class MappedHolder
    {
    public:
        MappedHolder(LRUResourceCache * cache_, const Key & key_, MappedPtr value_) : cache(cache_), key(key_), val(value_) { }

        ~MappedHolder() { cache->release(key); }

        Mapped & value() { return *val; }

    protected:
        LRUResourceCache * cache;
        Key key;
        MappedPtr val;
    };

    using MappedHolderPtr = std::unique_ptr<MappedHolder>;

    explicit LRUResourceCache(size_t max_weight_, size_t max_element_size_ = 0)
        : max_weight(max_weight_), max_element_size(max_element_size_)
    {
    }

    MappedHolderPtr get(const Key & key)
    {
        auto mapped_ptr = getImpl(key);
        if (!mapped_ptr)
            return nullptr;
        return std::make_unique<MappedHolder>(this, key, mapped_ptr);
    }

    template <typename LoadFunc>
    MappedHolderPtr getOrSet(const Key & key, LoadFunc && load_func)
    {
        auto mapped_ptr = getImpl(key, load_func);
        if (!mapped_ptr)
            return nullptr;
        return std::make_unique<MappedHolder>(this, key, mapped_ptr);
    }

    // If the key's reference_count = 0, delete it immediately.
    // Otherwise, mark it expired (not visible to get()), and delete when refcount is 0.
    void tryRemove(const Key & key)
    {
        std::lock_guard lock(mutex);
        auto it = cells.find(key);
        if (it == cells.end())
            return;
        auto & cell = it->second;
        if (cell.reference_count == 0)
        {
            queue.erase(cell.queue_iterator);
            current_weight -= cell.weight;
            release_function(cell.value);
            cells.erase(it);
        }
        else
            cell.expired = true;
    }

    size_t weight()
    {
        std::lock_guard lock(mutex);
        return current_weight;
    }

    size_t size()
    {
        std::lock_guard lock(mutex);
        return cells.size();
    }

    void getStats(size_t & out_hits, size_t & out_misses, size_t & out_evict_count) const
    {
        out_hits = hits;
        out_misses = misses;
        out_evict_count = evict_count;
    }

private:
    mutable std::mutex mutex;

    using LRUQueue = std::list<Key>;
    using LRUQueueIterator = typename LRUQueue::iterator;

    struct Cell
    {
        MappedPtr value;
        size_t weight = 0;
        LRUQueueIterator queue_iterator;
        size_t reference_count = 0;
        bool expired = false;
    };

    using Cells = std::unordered_map<Key, Cell, HashFunction>;
    Cells cells;
    LRUQueue queue;
    size_t current_weight = 0;
    size_t max_weight = 0;
    size_t max_element_size = 0;

    /// Represents pending insertion attempt.
    struct InsertToken
    {
        explicit InsertToken(LRUResourceCache & cache_) : cache(cache_) { }

        std::mutex mutex;
        bool cleaned_up = false; /// Protected by the token mutex
        MappedPtr value; /// Protected by the token mutex

        LRUResourceCache & cache;
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

        void
        acquire(const Key * key_, const std::shared_ptr<InsertToken> & token_, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock)
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
    WeightFunction weight_function;
    ReleaseFunction release_function;
    std::atomic<size_t> hits{0};
    std::atomic<size_t> misses{0};
    std::atomic<size_t> evict_count{0};

    /// Returns nullptr when there is no more space for the new value or the old value is in used.
    template <typename LoadFunc>
    MappedPtr getImpl(const Key & key, LoadFunc && load_func)
    {
        InsertTokenHolder token_holder;
        {
            std::lock_guard lock(mutex);
            auto it = cells.find(key);
            if (it != cells.end())
            {
                if (!it->second.expired)
                {
                    ++hits;
                    it->second.reference_count += 1;
                    queue.splice(queue.end(), queue, it->second.queue_iterator);
                    return it->second.value;
                }
                if (it->second.reference_count > 0)
                    return nullptr;

                // should not reach here
                LOG_ERROR(getLogger("LRUResourceCache"), "element is in invalid status.");
                abort();
            }
            ++misses;
            auto & token = insert_tokens[key];
            if (!token)
                token = std::make_shared<InsertToken>(*this);
            token_holder.acquire(&key, token, lock);
        }

        auto * token = token_holder.token.get();
        std::lock_guard token_lock(token->mutex);
        token_holder.cleaned_up = token->cleaned_up;

        if (!token->value)
            token->value = load_func();

        std::lock_guard lock(mutex);
        auto token_it = insert_tokens.find(key);
        Cell * cell_ptr = nullptr;
        if (token_it != insert_tokens.end() && token_it->second.get() == token)
        {
            cell_ptr = set(key, token->value);
        }
        else
        {
            auto cell_it = cells.find(key);
            if (cell_it != cells.end() && !cell_it->second.expired)
            {
                cell_ptr = &cell_it->second;
            }
        }

        if (!token->cleaned_up)
            token_holder.cleanup(token_lock, lock);

        if (cell_ptr)
        {
            queue.splice(queue.end(), queue, cell_ptr->queue_iterator);
            cell_ptr->reference_count++;
            return cell_ptr->value;
        }
        return nullptr;
    }

    MappedPtr getImpl(const Key & key)
    {
        std::lock_guard lock(mutex);

        auto it = cells.find(key);
        if (it == cells.end() || it->second.expired)
        {
            ++misses;
            return nullptr;
        }

        ++hits;
        it->second.reference_count += 1;
        queue.splice(queue.end(), queue, it->second.queue_iterator);
        return it->second.value;
    }

    // mark a reference is released
    void release(const Key & key)
    {
        std::lock_guard lock(mutex);

        auto it = cells.find(key);
        if (it == cells.end() || it->second.reference_count == 0)
        {
            LOG_ERROR(getLogger("LRUResourceCache"), "try to release an invalid element");
            abort();
        }

        auto & cell = it->second;
        cell.reference_count -= 1;
        if (cell.expired && cell.reference_count == 0)
        {
            queue.erase(cell.queue_iterator);
            current_weight -= cell.weight;
            release_function(cell.value);
            cells.erase(it);
        }
    }

    InsertToken * acquireInsertToken(const Key & key)
    {
        auto & token = insert_tokens[key];
        token.reference_count += 1;
        return &token;
    }

    void releaseInsertToken(const Key & key)
    {
        auto it = insert_tokens.find(key);
        if (it != insert_tokens.end())
        {
            it->second.reference_count -= 1;
            if (it->second.reference_count == 0)
                insert_tokens.erase(it);
        }
    }

    // key mustn't be in the cache
    Cell * set(const Key & insert_key, MappedPtr value)
    {
        size_t weight = value ? weight_function(*value) : 0;
        size_t queue_size = cells.size() + 1;
        size_t loss_weight = 0;
        auto is_overflow = [&] {
            return current_weight + weight > max_weight + loss_weight || (max_element_size != 0 && queue_size > max_element_size);
        };

        auto key_it = queue.begin();
        std::unordered_set<Key, HashFunction> to_release_keys;

        while (is_overflow() && queue_size > 1 && key_it != queue.end())
        {
            const Key & key = *key_it;

            auto cell_it = cells.find(key);
            if (cell_it == cells.end())
            {
                LOG_ERROR(getLogger("LRUResourceCache"), "LRUResourceCache became inconsistent. There must be a bug in it.");
                abort();
            }

            auto & cell = cell_it->second;
            if (cell.reference_count == 0)
            {
                loss_weight += cell.weight;
                queue_size--;
                to_release_keys.insert(key);
            }

            ++key_it;
        }

        if (is_overflow())
            return nullptr;

        if (loss_weight > current_weight + weight)
        {
            LOG_ERROR(getLogger("LRUResourceCache"), "LRUResourceCache became inconsistent. There must be a bug in it.");
            abort();
        }

        for (auto & key : to_release_keys)
        {
            auto & cell = cells[key];
            queue.erase(cell.queue_iterator);
            release_function(cell.value);
            cells.erase(key);
            ++evict_count;
        }

        current_weight = current_weight + weight - loss_weight;

        auto & new_cell = cells[insert_key];
        new_cell.value = value;
        new_cell.weight = weight;
        new_cell.queue_iterator = queue.insert(queue.end(), insert_key);
        return &new_cell;
    }
};
}
