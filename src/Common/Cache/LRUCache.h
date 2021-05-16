#pragma once

#include <Common/Cache/Cache.h>

namespace DB
{

/// Thread-safe cache that evicts entries which are not used for a long time.
/// WeightFunction is a functor that takes Mapped as a parameter and returns "weight" (approximate size)
/// of that value.
/// Cache starts to evict entries when their total weight exceeds max_size.
/// Value weight should not change after insertion.
template <typename TKey, typename TMapped, typename HashFunction = std::hash<TKey>, typename WeightFunction = ITrivialWeightFunction<TMapped>>
class ILRUCache : public Cache<TKey, TMapped, HashFunction, WeightFunction>
{
private:
    using Base = Cache<TKey, TMapped, HashFunction, WeightFunction>;

public:
    using Key = typename Base::Key;
    using Mapped = typename Base::Mapped;
    using MappedPtr = std::shared_ptr<Mapped>;

    ILRUCache(size_t max_size_)
        : Base(max_size_) {}


protected:
    using LRUQueue = std::list<Key>;
    using LRUQueueIterator = typename LRUQueue::iterator;

    struct LRUCell : public Base::Cell
    {
        LRUQueueIterator queue_iterator;
    };

    LRUQueue queue;

    void resetImpl() override
    {
        Base::resetImpl();
        queue.clear();
    }

    MappedPtr getImpl(const Key & key, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock) override
    {
        auto it = this->cells.find(key);
        if (it == this->cells.end())
        {
            return MappedPtr();
        }

        LRUCell & cell = it->second->as<LRUCell &>();

        /// Move the key to the end of the queue. The iterator remains valid.
        queue.splice(queue.end(), queue, cell.queue_iterator);

        return cell.value;
    }

    void setImpl(const Key & key, const MappedPtr & mapped, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock) override
    {
        auto [it, inserted] = cells.emplace(std::piecewise_construct,
            std::forward_as_tuple(key),
            std::forward_as_tuple());

        LRUCell & cell = it->second->as<LRUCell &>();

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
        cell.size = cell.value ? weight_function(*cell.value) : 0;
        current_size += cell.size;

        removeOverflow();
    }

    void removeOverflow()
    {
        size_t current_weight_lost = 0;
        size_t queue_size = cells.size();
        while ((current_size > max_size) && (queue_size > 1))
        {
            const Key & key = queue.front();
            deleteImpl(key);
        }


        if (current_size > (1ull << 63))
        {
            LOG_ERROR(&Poco::Logger::get("LRUCache"), "LRUCache became inconsistent. There must be a bug in it.");
            abort();
        }
    }

    virtual void deleteImpl(const Key & key)
    {
        auto it = cells.find(key);
        if (it == cells.end())
        {
            LOG_ERROR(&Poco::Logger::get("LRUCache"), "LRUCache became inconsistent. There must be a bug in it.");
            abort();
        }

        const auto & cell = it->second->as<LRUCell &>();

        this->current_size -= cell.size;
        current_weight_lost += cell.size;

        cells.erase(it);
        queue.pop_front();
        --queue_size;
    }

};


}
