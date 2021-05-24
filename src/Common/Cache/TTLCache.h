#pragma once

#include <queue>
#include <vector>

#include <Common/Cache/LRUCache.h>

namespace DB
{
template <
    typename TKey,
    typename TMapped,
    typename HashFunction = std::hash<TKey>,
    typename WeightFunction = ITrivialWeightFunction<TMapped>>
class TTLCache : virtual public Cache<TKey, TMapped, HashFunction, WeightFunction>
{
private:
    using Base = Cache<TKey, TMapped, HashFunction, WeightFunction>;
    using Clock = std::chrono::steady_clock;

public:
    using Key = typename Base::Key;
    using Mapped = typename Base::Mapped;
    using MappedPtr = std::shared_ptr<Mapped>;

protected:
    struct TTLQueueElem
    {
        Key key;
        UInt64 expires_at;
        UInt64 ttl;

        TTLQueueElem() = default;
        TTLQueueElem(const Key & key_, UInt64 expires_at_, UInt64 ttl_) : key(key_), expires_at(expires_at_), ttl(ttl_) { }

        bool operator<(const TTLQueueElem & rhs) const
        {
            return this->expires_at < rhs.expires_at || (this->expires_at == rhs.expires_at && this->key < rhs.key);
        }
        virtual ~TTLQueueElem() = default;
    };

    using TTLQueueElemPtr = std::shared_ptr<TTLQueueElem>;


    using TTLQueue = std::set<TTLQueueElem>;

    using TTLQueueIterator = typename TTLQueue::iterator;

    struct Cell : virtual public Base::Cell
    {
        TTLQueueIterator ttl_queue_iterator;
    };

    TTLQueue ttl_queue;


public:
    virtual void resetImpl() override
    {
        Base::resetImpl();
        ttl_queue.clear();
    }

    virtual void removeInvalid() override
    {
        UInt64 now_ = now();
        auto it = ttl_queue.begin();
        for (; it != ttl_queue.end(); ++it)
        {
            if (it->expires_at > now_)
                break;
            this->deleteImpl(it->key);
        }

        ttl_queue.erase(ttl_queue.begin(), it);
    }

    virtual void updateStructuresGetOrSet(
        [[maybe_unused]] const Key & key,
        [[maybe_unused]] std::shared_ptr<typename Base::Cell> & cell_ptr,
        [[maybe_unused]] bool new_element = false,
        [[maybe_unused]] UInt64 ttl = 2) override
    {
        auto cell = std::dynamic_pointer_cast<Cell>(cell_ptr);

        TTLQueueElem new_elem{key, now() + ttl, ttl};
        if (!new_element)
        {
            new_elem.ttl = cell->ttl_queue_iterator->ttl;
            new_elem.expires_at = now() + new_elem.ttl;
            ttl_queue.erase(cell->ttl_queue_iterator);
        }
        auto [it, inserted] = ttl_queue.insert(std::move(new_elem));
        cell->ttl_queue_iterator = it;
    }

    virtual void updateStructuresDelete([[maybe_unused]] const Key & key, [[maybe_unused]] std::shared_ptr<typename Base::Cell> & cell_ptr) override
    {
        auto cell = std::dynamic_pointer_cast<Cell>(cell_ptr);

        ttl_queue.erase(cell->ttl_queue_iterator);
    }

    static UInt64 now() { return std::chrono::duration_cast<std::chrono::seconds>(Clock::now().time_since_epoch()).count(); }
};

}
