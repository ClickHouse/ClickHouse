#pragma once

#include <Common/Cache/Cache.h>

namespace DB
{
template <
    typename TKey,
    typename TMapped,
    typename HashFunction = std::hash<TKey>,
    typename WeightFunction = ITrivialWeightFunction<TMapped>>
class ILRUCache : virtual public Cache<TKey, TMapped, HashFunction, WeightFunction>
{
private:
    using Base = Cache<TKey, TMapped, HashFunction, WeightFunction>;

public:
    using Key = typename Base::Key;
    using Mapped = typename Base::Mapped;
    using MappedPtr = std::shared_ptr<Mapped>;

    explicit ILRUCache(size_t max_size_) : Base(max_size_) { }


protected:
    using LRUQueue = std::list<Key>;
    using LRUQueueIterator = typename LRUQueue::iterator;

    struct Cell : virtual public Base::Cell
    {
        LRUQueueIterator queue_iterator;
    };

    LRUQueue queue;

    void resetImpl() override
    {
        Base::resetImpl();
        queue.clear();
    }

    virtual void removeInvalid() override { }

    virtual void removeOverflow() override
    {
        while ((Base::current_size > Base::max_size) && (queue.size()))
        {
            const Key & key = queue.front();
            this->deleteImpl(key);
        }

        if (Base::current_size > (1ull << 63))
        {
            LOG_ERROR(&Poco::Logger::get("LRUCache"), "LRUCache became inconsistent. There must be a bug in it.");
            abort();
        }
    }

    virtual void updateStructuresGetOrSet(
        [[maybe_unused]] const Key & key,
        [[maybe_unused]] std::shared_ptr<typename Base::Cell> & cell_ptr,
        [[maybe_unused]] bool new_element = false,
        [[maybe_unused]] UInt64 ttl = 2) override
    {
        auto cell = std::dynamic_pointer_cast<ILRUCache::Cell>(cell_ptr);
        if (new_element)
        {
            cell->queue_iterator = queue.insert(queue.end(), key);
        }
        else
        {
            /// Move the key to the end of the queue. The iterator remains valid.
            queue.splice(queue.end(), queue, cell->queue_iterator);
        }
    }

    virtual void updateStructuresDelete([[maybe_unused]] const Key & key, [[maybe_unused]] std::shared_ptr<typename Base::Cell> & cell_ptr) override { queue.pop_front(); }
};


}
