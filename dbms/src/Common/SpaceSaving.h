#pragma once

#include <iostream>
#include <vector>

#include <boost/range/adaptor/reversed.hpp>

#include <Common/ArenaWithFreeLists.h>
#include <Common/UInt128.h>
#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashMap.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>

/*
 * Implementation of the Filtered Space-Saving for TopK streaming analysis.
 *   http://www.l2f.inesc-id.pt/~fmmb/wiki/uploads/Work/misnis.ref0a.pdf
 * It implements suggested reduce-and-combine algorithm from Parallel Space Saving:
 *   https://arxiv.org/pdf/1401.0702.pdf
 */

namespace DB
{

/*
 * Arena interface to allow specialized storage of keys.
 * POD keys do not require additional storage, so this interface is empty.
 */
template <typename TKey>
struct SpaceSavingArena
{
    SpaceSavingArena() {}
    const TKey emplace(const TKey & key) { return key; }
    void free(const TKey & /*key*/) {}
};

/*
 * Specialized storage for StringRef with a freelist arena.
 * Keys of this type that are retained on insertion must be serialized into local storage,
 * otherwise the reference would be invalid after the processed block is released.
 */
template <>
struct SpaceSavingArena<StringRef>
{
    const StringRef emplace(const StringRef & key)
    {
        auto ptr = arena.alloc(key.size);
        std::copy(key.data, key.data + key.size, ptr);
        return StringRef{ptr, key.size};
    }

    void free(const StringRef & key)
    {
        if (key.data)
            arena.free(const_cast<char *>(key.data), key.size);
    }

private:
    ArenaWithFreeLists arena;
};


template
<
    typename TKey,
    typename Hash = DefaultHash<TKey>,
    typename Grower = HashTableGrower<>,
    typename Allocator = HashTableAllocator
>
class SpaceSaving
{
private:
    // Suggested constants in the paper "Finding top-k elements in data streams", chap 6. equation (24)
    // Round to nearest power of 2 for cheaper binning without modulo
    constexpr uint64_t nextAlphaSize(uint64_t x)
    {
        constexpr uint64_t ALPHA_MAP_ELEMENTS_PER_COUNTER = 6;
        return 1ULL << (sizeof(uint64_t) * 8 - __builtin_clzll(x * ALPHA_MAP_ELEMENTS_PER_COUNTER));
    }

public:
    using Self = SpaceSaving;

    struct Counter
    {
        Counter() {}

        Counter(const TKey & k, UInt64 c = 0, UInt64 e = 0, size_t h = 0)
          : key(k), slot(0), hash(h), count(c), error(e) {}

        void write(WriteBuffer & wb) const
        {
            writeBinary(key, wb);
            writeVarUInt(count, wb);
            writeVarUInt(error, wb);
        }

        void read(ReadBuffer & rb)
        {
            readBinary(key, rb);
            readVarUInt(count, rb);
            readVarUInt(error, rb);
        }

        // greater() taking slot error into account
        bool operator> (const Counter & b) const
        {
            return (count > b.count) || (count == b.count && error < b.error);
        }

        TKey key;
        size_t slot, hash;
        UInt64 count;
        UInt64 error;
    };

    SpaceSaving(size_t c = 10) : alpha_map(nextAlphaSize(c)), m_capacity(c) {}

    ~SpaceSaving() { destroyElements(); }

    inline size_t size() const
    {
        return counter_list.size();
    }

    inline size_t capacity() const
    {
        return m_capacity;
    }

    void clear()
    {
        return destroyElements();
    }

    void resize(size_t new_capacity)
    {
        counter_list.reserve(new_capacity);
        alpha_map.resize(nextAlphaSize(new_capacity));
        m_capacity = new_capacity;
    }

    void insert(const TKey & key, UInt64 increment = 1, UInt64 error = 0)
    {
        // Increase weight of a key that already exists
        // It uses hashtable for both value mapping as a presence test (c_i != 0)
        auto hash = counter_map.hash(key);
        auto it = counter_map.find(key, hash);
        if (it != counter_map.end())
        {
            auto c = it->getSecond();
            c->count += increment;
            c->error += error;
            percolate(c);
            return;
        }
        // Key doesn't exist, but can fit in the top K
        else if (unlikely(size() < capacity()))
        {
            auto c = new Counter(arena.emplace(key), increment, error, hash);
            push(c);
            return;
        }

        auto min = counter_list.back();
        const size_t alpha_mask = alpha_map.size() - 1;
        auto & alpha = alpha_map[hash & alpha_mask];
        if (alpha + increment < min->count)
        {
            alpha += increment;
            return;
        }

        // Erase the current minimum element
        alpha_map[min->hash & alpha_mask] = min->count;
        it = counter_map.find(min->key, min->hash);

        // Replace minimum with newly inserted element
        if (it != counter_map.end())
        {
            arena.free(min->key);
            min->hash = hash;
            min->key = arena.emplace(key);
            min->count = alpha + increment;
            min->error = alpha + error;
            percolate(min);

            it->getSecond() = min;
            it->getFirstMutable() = min->key;
            counter_map.reinsert(it, hash);
        }
    }

    /*
     * Parallel Space Saving reduction and combine step from:
     *  https://arxiv.org/pdf/1401.0702.pdf
     */
    void merge(const Self & rhs)
    {
        UInt64 m1 = 0;
        UInt64 m2 = 0;

        if (size() == capacity())
        {
            m1 = counter_list.back()->count;
        }

        if (rhs.size() == rhs.capacity())
        {
            m2 = rhs.counter_list.back()->count;
        }

        /*
         * Updated algorithm to mutate current table in place
         * without mutating rhs table or creating new one
         * in the first step we expect that no elements overlap
         * and in the second sweep we correct the error if they do.
         */
        if (m2 > 0)
        {
            for (auto counter : counter_list)
            {
                counter->count += m2;
                counter->error += m2;
            }
        }

        // The list is sorted in descending order, we have to scan in reverse
        for (auto counter : boost::adaptors::reverse(rhs.counter_list))
        {
            if (counter_map.find(counter->key) != counter_map.end())
            {
                // Subtract m2 previously added, guaranteed not negative
                insert(counter->key, counter->count - m2, counter->error - m2);
            }
            else
            {
                // Counters not monitored in S1
                insert(counter->key, counter->count + m1, counter->error + m1);
            }
        }
    }

    std::vector<Counter> topK(size_t k) const
    {
        std::vector<Counter> res;
        for (auto counter : counter_list)
        {
            res.push_back(*counter);
            if (res.size() == k)
                break;
        }
        return res;
    }

    void write(WriteBuffer & wb) const
    {
        writeVarUInt(size(), wb);
        for (auto counter : counter_list)
            counter->write(wb);

        writeVarUInt(alpha_map.size(), wb);
        for (auto alpha : alpha_map)
            writeVarUInt(alpha, wb);
    }

    void read(ReadBuffer & rb)
    {
        destroyElements();
        size_t count = 0;
        readVarUInt(count, rb);

        for (size_t i = 0; i < count; ++i)
        {
            auto counter = new Counter();
            counter->read(rb);
            counter->hash = counter_map.hash(counter->key);
            push(counter);
        }

        readAlphaMap(rb);
    }

    void readAlphaMap(ReadBuffer & rb)
    {
        size_t alpha_size = 0;
        readVarUInt(alpha_size, rb);
        for (size_t i = 0; i < alpha_size; ++i)
        {
            UInt64 alpha = 0;
            readVarUInt(alpha, rb);
            alpha_map.push_back(alpha);
        }
    }

protected:
    void push(Counter * counter)
    {
        counter->slot = counter_list.size();
        counter_list.push_back(counter);
        counter_map[counter->key] = counter;
        percolate(counter);
    }

    // This is equivallent to one step of bubble sort
    void percolate(Counter * counter)
    {
        while (counter->slot > 0)
        {
            auto next = counter_list[counter->slot - 1];
            if (*counter > *next)
            {
                std::swap(next->slot, counter->slot);
                std::swap(counter_list[next->slot], counter_list[counter->slot]);
            }
            else
                break;
        }
    }

private:
    void destroyElements()
    {
        for (auto counter : counter_list)
            delete counter;

        counter_map.clear();
        counter_list.clear();
        alpha_map.clear();
    }

    HashMap<TKey, Counter *, Hash, Grower, Allocator> counter_map;
    std::vector<Counter *> counter_list;
    std::vector<UInt64> alpha_map;
    SpaceSavingArena<TKey> arena;
    size_t m_capacity;
};

}
