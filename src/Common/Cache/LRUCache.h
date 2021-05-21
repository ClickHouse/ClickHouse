#pragma once

#include <Common/Cache/Cache.h>

namespace DB
{

template <typename TKey, typename TMapped, typename HashFunction = std::hash<TKey>, typename WeightFunction = ITrivialWeightFunction<TMapped>>
class ILRUCache : public Cache<TKey, TMapped, HashFunction, WeightFunction>
{
private:
    using Base = Cache<TKey, TMapped, HashFunction, WeightFunction>;

public:
    using Key = typename Base::Key;
    using Mapped = typename Base::Mapped;
    using MappedPtr = std::shared_ptr<Mapped>;

    explicit ILRUCache(size_t max_size_)
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

    virtual MappedPtr getImpl(const Key & key, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock) override
    {
        auto it = Base::cells.find(key);
        if (it == Base::cells.end())
        {
            return MappedPtr();
        }
        auto cell = std::dynamic_pointer_cast<LRUCell>(it->second);

        /// Move the key to the end of the queue. The iterator remains valid.
        queue.splice(queue.end(), queue, cell->queue_iterator);

        return cell->value;
    }

    virtual void setImpl(const Key & key, const MappedPtr & mapped, [[maybe_unused]] std::lock_guard<std::mutex> & cache_lock) override
    {
        auto [it, inserted] = Base::cells.emplace(std::piecewise_construct,
            std::forward_as_tuple(key),
            std::forward_as_tuple());

        auto cell = std::dynamic_pointer_cast<LRUCell>(it->second);

        if (inserted)
        {
            try
            {
                cell->queue_iterator = queue.insert(queue.end(), key);
            }
            catch (...)
            {
                Base::cells.erase(it);
                throw;
            }
        }
        else
        {
            Base::current_size -= cell->size;
            queue.splice(queue.end(), queue, cell->queue_iterator);
        }

        cell->value = mapped;
        cell->size = cell->value ? Base::weight_function(*cell->value) : 0;
        Base::current_size += cell->size;

        removeOverflow();
    }

    void removeOverflow()
    {
        while ((Base::current_size > Base::max_size) && (queue.size()))
        {
            const Key & key = queue.front();
            deleteImpl(key);
        }


        if (Base::current_size > (1ull << 63))
        {
            LOG_ERROR(&Poco::Logger::get("LRUCache"), "LRUCache became inconsistent. There must be a bug in it.");
            abort();
        }
    }

    virtual void deleteImpl(const Key & key) override
    {
        auto it = Base::cells.find(key);
        if (it == Base::cells.end())
        {
            LOG_ERROR(&Poco::Logger::get("LRUCache"), "LRUCache became inconsistent. There must be a bug in it.");
            abort();
        }

        const auto cell = std::dynamic_pointer_cast<LRUCell>(it->second);

        Base::current_size -= cell->size;

        Base::cells.erase(it);
        queue.pop_front();
    }

};


}
