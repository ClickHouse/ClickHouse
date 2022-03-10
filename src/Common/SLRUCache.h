#pragma once 

#include <Common/LRUCache.h>

namespace DB
{

template <typename T>
struct TrivialSLRUCacheWeightFunction
{
    size_t operator()(const T &) const
    {
        return 1;
    }
};


template <typename TKey, typename TMapped, typename HashFunction = std::hash<TKey>, typename WeightFunction = TrivialSLRUCacheWeightFunction<TMapped>>
class SLRUCache: public LRUCache<TKey, TMapped, HashFunction, WeightFunction>
{
public:
    using Key = TKey;
    using Mapped = TMapped;
    using MappedPtr = std::shared_ptr<Mapped>;
    using Base = LRUCache<Key, Mapped, HashFunction, WeightFunction>;
    using Base::mutex;

    SLRUCache(size_t max_protected_size_, size_t max_size_)
        : Base(0)
        , max_protected_size(std::max(static_cast<size_t>(1), max_protected_size_))
        , max_size(std::max(max_protected_size + 1, max_size_))
        {}

    void remove(const Key & key)
    {
        std::lock_guard lock(mutex);
        auto it = cells.find(key);
        if (it == cells.end())
            return;
        auto & cell = it->second;
        current_size -= cell.size;
        auto & queue = cell.is_protected ? protected_queue : probationary_queue;
        queue.erase(it);
        cells.erase(it);
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

    size_t maxProtectedSize() const
    {
        return max_protected_size;
    }

    void reset()
    {
        std::lock_guard lock(mutex);
        resetImpl();
    }

protected:
    using SLRUQueue = std::list<Key>;
    using SLRUQueueIterator = typename SLRUQueue::iterator;

    struct Cell
    {
        bool is_protected;
        MappedPtr value;
        size_t size;
        SLRUQueueIterator queue_iterator;
    };

    using Cells = std::unordered_map<Key, Cell, HashFunction>;

    Cells cells;

    void resetImpl()
    {
        cells.clear();
        probationary_queue.clear();
        protected_queue.clear();
        current_size = 0;
        current_protected_size = 0;
        Base::resetImpl();
    }

private:
    SLRUQueue probationary_queue;
    SLRUQueue protected_queue;

    size_t current_protected_size = 0;
    size_t current_size = 0;
    const size_t max_protected_size;
    const size_t max_size;
    
    WeightFunction weight_function;

    MappedPtr getImpl(const Key & key, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock) override
    {
        auto it = cells.find(key);
        if (it == cells.end())
        {
            return MappedPtr();
        }

        Cell & cell = it->second;

        if (cell.is_protected)
        {
            protected_queue.splice(protected_queue.end(), protected_queue, cell.queue_iterator);
        }
        else
        {
            cell.is_protected = true;
            current_protected_size += cell.size;
            protected_queue.splice(protected_queue.end(), probationary_queue, cell.queue_iterator);
        }

        removeOverflow(protected_queue, max_protected_size, current_protected_size);

        return cell.value;
    }

    void setImpl(const Key & key, const MappedPtr & mapped, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock) override
    {
        auto [it, inserted] = cells.emplace(std::piecewise_construct,
            std::forward_as_tuple(key),
            std::forward_as_tuple());

        Cell & cell = it->second;

        if (inserted)
        {
            try
            {
                cell.queue_iterator = probationary_queue.insert(probationary_queue.end(), key);
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
            if (cell.is_protected)
            {
                current_protected_size -= cell.size;
                protected_queue.splice(protected_queue.end(), protected_queue, cell.queue_iterator);
            }
            else
            {
                cell.is_protected = true;
                protected_queue.splice(protected_queue.end(), probationary_queue, cell.queue_iterator);
            }
        }

        cell.value = mapped;
        cell.size = cell.value ? weight_function(*cell.value) : 0;
        current_size += cell.size;
        current_protected_size += cell.is_protected ? cell.size : 0;

        removeOverflow(protected_queue, max_protected_size, current_protected_size);
        removeOverflow(probationary_queue, max_size, current_size);
    }

    void removeOverflow(SLRUQueue & queue, const size_t max_weight_size, size_t & current_weight_size)
    {
        size_t current_weight_lost = 0;
        size_t queue_size = queue.size();

        while (current_weight_size > max_weight_size && queue_size > 1)
        {
            const Key & key = queue.front();

            auto it = cells.find(key);
            if (it == cells.end())
            {
                LOG_ERROR(&Poco::Logger::get("LRUCache"), "LRUCache became inconsistent. There must be a bug in it.");
                abort();
            }

            auto & cell = it->second;

            current_weight_size -= cell.size;

            if (cell.is_protected)
            {
                cell.is_protected = false;
                probationary_queue.splice(probationary_queue.end(), queue, cell.queue_iterator);
            }
            else
            {
                current_weight_lost += cell.size;
                cells.erase(it);
                queue.pop_front();
            }

            --queue_size;
        }

        onRemoveOverflowWeightLoss(current_weight_lost);

        if (current_size > (1ull << 63))
        {
            LOG_ERROR(&Poco::Logger::get("LRUCache"), "LRUCache became inconsistent. There must be a bug in it.");
            abort();
        }
    }

    
    /// Override this method if you want to track how much weight was lost in removeOverflow method.
    virtual void onRemoveOverflowWeightLoss(size_t /*weight_loss*/) override {}
};


}
