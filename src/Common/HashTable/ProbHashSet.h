#pragma once

#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashTableAllocator.h>
#include <Common/HashTable/DynamicBloomFilter.h>
#include <Common/HashTable/CuckooFilter.h>

template <typename Key, typename Hash = DefaultHash<Key>, typename Allocator = HashTableAllocator>
class ProbHashSetBloomFilter
{
public:
    using key_type = Key;
    using value_type = Key;
    using LookupResult = const bool*;

    explicit ProbHashSetBloomFilter(double targetFPR_ = 0.001) : filter(targetFPR_), targetFPR(targetFPR_)
    {
    }

    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder && key_holder, LookupResult &, bool & inserted)
    {
        filter.insert(keyHolderGetKey(key_holder));
        ++filter_size;
        inserted = true;
    }

    LookupResult ALWAYS_INLINE find(const Key & key)
    {
        return filter.lookup(key) ? &lookup_result : nullptr;
    }

    size_t size() const
    {
        return filter_size;
    }

    size_t getBufferSizeInBytes() const
    {
        return filter.getBufferSizeInBytes();
    }

    void clear()
    {
        filter.clear();
        filter_size = 0;
    }

private:
    constexpr static bool lookup_result = true;

    DynamicBloomFilter<Key, Hash, Allocator> filter;
    size_t filter_size = 0;
    double targetFPR;
};

template <typename Key, typename Hash = DefaultHash<Key>, typename Allocator = HashTableAllocator>
class ProbHashSetCuckooFilter
{
public:
    using key_type = Key;
    using value_type = Key;
    using LookupResult = const bool*;

    explicit ProbHashSetCuckooFilter(double targetFPR_ = 0.001) : targetFPR(targetFPR_)
    {
        filters.emplace_back(targetFPR);
    }

    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder && key_holder, LookupResult &, bool & inserted)
    {
        auto key = keyHolderGetKey(key_holder);

        ++filter_size;
        inserted = true;

        if (!filters.back().insert(key))
        {
            filters.emplace_back(targetFPR);
            filters.back().insert(key);
        }
    }

    LookupResult ALWAYS_INLINE find(const Key& key)
    {
        for (const auto& filter : filters)
        {
            if (filter.lookup(key))
            {
                return &lookup_result;
            }
        }
        return nullptr;
    }

    size_t size() const
    {
        return filter_size;
    }

    size_t getBufferSizeInBytes() const
    {
        size_t result = 0;
        for (const auto& filter : filters)
        {
            result += filter.getBufferSizeInBytes();
        }
        return result;
    }

    void clear()
    {
        filters.clear();
        filter_size = 0;

        filters.emplace_back(targetFPR);
    }

private:
    constexpr static bool lookup_result = true;

    std::vector<CuckooFilter<Key, Hash, Allocator>> filters;
    size_t filter_size = 0;
    double targetFPR;
};
