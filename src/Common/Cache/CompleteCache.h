#pragma once

#include <Common/Cache/Cache.h>
#include <Common/Cache/LRUCache.h>
#include <Common/Cache/LFUCache.h>
#include <Common/Cache/TTLCache.h>

namespace DB
{
template <
    typename TKey,
    typename TMapped,
    typename HashFunction = std::hash<TKey>,
    typename WeightFunction = ITrivialWeightFunction<TMapped>,
    class CacheEviction = ILRUCache<TKey, TMapped, HashFunction, WeightFunction>,
    class CacheInvalidation = TTLCache<TKey, TMapped, HashFunction, WeightFunction>>
class CompleteCache : public CacheEviction, public CacheInvalidation
{
private:
    using Base = Cache<TKey, TMapped, HashFunction, WeightFunction>;

public:
    using Key = TKey;
    using Mapped = TMapped;
    using MappedPtr = std::shared_ptr<Mapped>;

    explicit CompleteCache(size_t max_size_ = 1u) : CacheEviction(max_size_), CacheInvalidation() { }


protected:
    struct Cell : public CacheEviction::Cell, public CacheInvalidation::Cell
    {
    };

    virtual void resetImpl() override
    {
        CacheEviction::resetImpl();
        CacheInvalidation::resetImpl();
    }

    virtual void removeInvalid() override { CacheInvalidation::removeInvalid(); }

    virtual void removeOverflow() override { CacheEviction::removeOverflow(); }

    virtual void updateStructuresGetOrSet(
        [[maybe_unused]] const Key & key,
        [[maybe_unused]] std::shared_ptr<typename Base::Cell> & cell_ptr,
        [[maybe_unused]] bool new_element,
        [[maybe_unused]] UInt64 ttl = 2) override
    {
        CacheInvalidation::updateStructuresGetOrSet(key, cell_ptr, new_element, ttl);
        CacheEviction::updateStructuresGetOrSet(key, cell_ptr, new_element, ttl);
    }

    virtual void updateStructuresDelete(const Key & key, std::shared_ptr<typename Base::Cell> & cell_ptr) override
    {
        CacheInvalidation::updateStructuresDelete(key, cell_ptr);
        CacheEviction::updateStructuresDelete(key, cell_ptr);
    }
};

template <
    typename TKey,
    typename TMapped,
    typename HashFunction = std::hash<TKey>,
    typename WeightFunction = ITrivialWeightFunction<TMapped>>
using TTLLRUCache = CompleteCache<TKey, TMapped, HashFunction, WeightFunction>;


template <
    typename TKey,
    typename TMapped,
    typename HashFunction = std::hash<TKey>,
    typename WeightFunction = ITrivialWeightFunction<TMapped>>
using TTLLFUCache = CompleteCache<TKey, TMapped, HashFunction, WeightFunction, LFUCache<TKey, TMapped, HashFunction, WeightFunction>>;
}
