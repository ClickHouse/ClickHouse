#pragma once

#include <atomic>
#include <chrono>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <base/logger_useful.h>

namespace DB
{
template <typename T>
struct TrivailLRUResourceCacheWeightFunction
{
    size_t operator()(const T &) const { return 1; }
};

/*
 * A resource cache with key index. There is only one instance for every key which is not like the normal resource pool.
 * Resource cache has max weight capacity and keys size limitation. If the limitation is exceeded, keys would be evicted
 * by LRU policy.
 *
 * acquire and release must be used in pair.
 */
template <
    typename TKey,
    typename TMapped,
    typename WeightFunction = TrivailLRUResourceCacheWeightFunction<TMapped>,
    typename HashFunction = std::hash<TKey>>
class LRUResourceCache
{
public:
    LRUResourceCache(size_t max_weight_, size_t max_element_size_ = 0) : max_weight(max_weight_), max_element_size(max_element_size_) { }
    virtual ~LRUResourceCache() { }
    using Key = TKey;
    using Mapped = TMapped;
    using MappedPtr = std::shared_ptr<Mapped>;

    // - load_func : when key is not exists in cache, load_func is called to generate a new key
    // - return: is null when there is no more space for the new value or the old value is in used.
    template <typename LoadFunc>
    MappedPtr acquire(const Key & key, LoadFunc && load_func)
    {
        InsertToken * insert_token = nullptr;
        {
            std::lock_guard lock(mutex);
            auto it = cells.find(key);
            if (it != cells.end())
            {
                hits++;
                it->second.reference_count += 1;
                return it->second.value;
            }
            misses++;
            insert_token = acquireInsertToken(key);
        }
        Cell * cell_ptr = nullptr;
        {
            std::lock_guard lock(insert_token->mutex);
            if (!insert_token->value)
            {
                insert_token->value = load_func();
                std::lock_guard cell_lock(mutex);
                cell_ptr = insert_value(key, insert_token->value);
                if (cell_ptr)
                {
                    cell_ptr->reference_count += 1;
                }
                else
                {
                    insert_token->value = nullptr;
                }
            }
        }

        std::lock_guard lock(mutex);
        releaseInsertToken(key);
        if (cell_ptr)
        {
            return cell_ptr->value;
        }
        return nullptr;
    }

    MappedPtr acquire(const Key & key)
    {
        std::lock_guard lock(mutex);
        auto it = cells.find(key);
        if (it == cells.end())
        {
            misses++;
            return nullptr;
        }
        hits++;
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
            LOG_ERROR(&Poco::Logger::get("LRUResourceCache"), "try to release an invalid element");
            abort();
        }
        it->second.reference_count -= 1;
    }

    // If you want to update a value, call tryRemove() at first and then call acquire() with load_func.
    bool tryRemove(const Key & key)
    {
        std::lock_guard guard(mutex);
        auto it = cells.find(key);
        if (it == cells.end())
            return true;
        auto & cell = it->second;
        if (cell.reference_count)
            return false;
        queue.erase(cell.queue_iterator);
        current_weight -= cell.weight;
        cells.erase(it);
        return true;
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
    };

    using Cells = std::unordered_map<Key, Cell, HashFunction>;
    Cells cells;
    LRUQueue queue;
    size_t current_weight = 0;
    size_t max_weight = 0;
    size_t max_element_size = 0;

    struct InsertToken
    {
        std::mutex mutex;
        MappedPtr value;
        size_t reference_count = 0;
    };
    using InsertTokens = std::unordered_map<Key, InsertToken, HashFunction>;
    InsertTokens insert_tokens;
    WeightFunction weight_function;
    std::atomic<size_t> hits{0};
    std::atomic<size_t> misses{0};
    std::atomic<size_t> evict_count{0};

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
    Cell * insert_value(const Key & insert_key, MappedPtr value)
    {
        auto weight = value ? weight_function(*value) : 0;
        auto queue_size = cells.size() + 1;
        auto loss_weight = 0;
        auto is_overflow = [&] {
            return current_weight + weight - loss_weight > max_weight || (max_element_size != 0 && queue_size > max_element_size);
        };
        auto key_it = queue.begin();
        std::unordered_set<Key, HashFunction> to_release_keys;
        while (is_overflow() && queue_size > 1 && key_it != queue.end())
        {
            const Key & key = *key_it;
            auto cell_it = cells.find(key);
            if (cell_it == cells.end())
            {
                LOG_ERROR(&Poco::Logger::get("LRUResourceCache"), "LRUResourceCache became inconsistent. There must be a bug in it.");
                abort();
            }
            auto & cell = cell_it->second;
            if (cell.reference_count == 0)
            {
                loss_weight += cell.weight;
                queue_size -= 1;
                to_release_keys.insert(key);
            }
            key_it++;
        }
        if (is_overflow())
            return nullptr;
        if (loss_weight > current_weight + weight)
        {
            LOG_ERROR(&Poco::Logger::get("LRUResourceCache"), "LRUResourceCache became inconsistent. There must be a bug in it.");
            abort();
        }
        for (auto & key : to_release_keys)
        {
            auto & cell = cells[key];
            queue.erase(cell.queue_iterator);
            cells.erase(key);
            evict_count++;
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
