#pragma once

#include <IO/VarInt.h>
#include <base/sort.h>
#include <Common/AllocatorWithMemoryTracking.h>
#include <Common/ArenaUtils.h>
#include <Common/ArenaWithFreeLists.h>
#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashMap.h>

#include <vector>


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
    SpaceSavingArena() = default;
    TKey emplace(const TKey & key) { return key; }
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
    StringRef emplace(StringRef key)
    {
        if (!key.data)
            return key;

        return copyStringInArena(arena, key);
    }

    void free(StringRef key)
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
    typename Hash = DefaultHash<TKey>
>
class SpaceSaving
{
private:
    // Suggested constants in the paper "Finding top-k elements in data streams", chap 6. equation (24)
    // Round to nearest power of 2 for cheaper binning without modulo
    constexpr UInt64 nextAlphaSize(UInt64 x)
    {
        constexpr UInt64 alpha_map_elements_per_counter = 6;
        return 1ULL << (sizeof(UInt64) * 8 - std::countl_zero(x * alpha_map_elements_per_counter));
    }

public:
    using Self = SpaceSaving;

    struct Counter
    {
        Counter() = default;

        explicit Counter(const TKey & k, UInt64 c = 0, UInt64 e = 0, size_t h = 0)
            : key(k)
            , hash(h)
            , count(c)
            , error(e)
        {
        }

        void write(WriteBuffer & wb) const
        {
            if constexpr (std::is_same_v<TKey, StringRef>)
                writeBinary(key, wb);
            else
                writeBinaryLittleEndian(key, wb);
            writeVarUInt(count, wb);
            writeVarUInt(error, wb);
        }

        void read(ReadBuffer & rb)
        {
            if constexpr (std::is_same_v<TKey, StringRef>)
                readBinary(key, rb);
            else
                readBinaryLittleEndian(key, rb);
            readVarUInt(count, rb);
            readVarUInt(error, rb);
        }

        // greater() taking error into account
        bool operator> (const Counter & b) const
        {
            return ((count - error) > (b.count - b.error)) || ((count - error) == (b.count - b.error) && error < b.error);
        }

        TKey key;
        size_t hash;
        UInt64 count;
        UInt64 error;
    };

    explicit SpaceSaving(size_t c = 10) : alpha_map(nextAlphaSize(c)), m_capacity(c) {}

    ~SpaceSaving() { destroyElements(); }

    bool empty() const { return counter_list.size() == 0; }

    size_t size() const
    {
        return counter_list.size();
    }

    size_t capacity() const
    {
        return m_capacity;
    }

    void clear()
    {
        return destroyElements();
    }

    void resize(size_t new_capacity)
    {
        counter_list.reserve(new_capacity * 2);
        alpha_map.resize(nextAlphaSize(new_capacity * 2));
        m_capacity = new_capacity;
    }

    void insert(const TKey & key, UInt64 increment = 1, UInt64 error = 0)
    {
        // Increase weight of a key that already exists
        auto hash = counter_map.hash(key);

        if (auto * counter = findCounter(key, hash); counter)
        {
            counter->count += increment;
            counter->error += error;
            return;
        }

        // Key doesn't exist, but can fit in the top K
        if (unlikely(counter_list.size() < capacity()))
        {
            push(Counter{arena.emplace(key), increment, error, hash});
            return;
        }

        const size_t alpha_mask = alpha_map.size() - 1;
        auto & alpha = alpha_map[hash & alpha_mask];
        push(Counter{arena.emplace(key), alpha + increment, alpha + error, hash});
    }

    /*
     * Parallel Space Saving reduction and combine step from:
     *  https://arxiv.org/pdf/1401.0702.pdf
     */
    void merge(const Self & rhs)
    {
        if (rhs.empty())
            return;

        UInt64 m1 = 0;
        UInt64 m2 = 0;

        if (size() >= capacity())
        {
            m1 = *std::ranges::max_element(alpha_map);
        }

        if (rhs.counter_list.size() >= rhs.capacity())
        {
            m2 = *std::ranges::max_element(rhs.alpha_map);
        }

        /*
         * Updated algorithm to mutate current table in place
         * without mutating rhs table or creating new one
         * in the first step we expect that no elements overlap
         * and in the second sweep we correct the error if they do.
         */
        if (m2 > 0)
        {
            for (auto & counter : counter_list)
            {
                counter.count += m2;
                counter.error += m2;
            }
        }

        // The list is sorted in descending order, we have to scan in reverse
        for (const auto & counter : rhs.counter_list)
        {
            size_t hash = counter.hash;
            if (auto * current = findCounter(counter.key, hash))
            {
                // Subtract m2 previously added, guaranteed not negative
                current->count += (counter.count - m2);
                current->error += (counter.error - m2);
            }
            else
            {
                // Counters not monitored in S1
                counter_list.push_back(Counter{arena.emplace(counter.key), counter.count + m1, counter.error + m1, hash});
            }
        }

        for (size_t i = 0, max_size = std::max(alpha_map.size(), rhs.alpha_map.size()); i < max_size; ++i)
        {
            alpha_map[i] = std::max(alpha_map[i], rhs.alpha_map[i]);
        }

        truncateIfNeeded();
    }

    std::vector<Counter> topK(size_t k) const
    {
        std::vector<Counter> new_list;
        new_list.reserve(counter_list.size());
        for (auto & counter : counter_list)
            new_list.push_back(counter);

        size_t return_size = std::min(new_list.size(), k);
        ::partial_sort(
            new_list.begin(), new_list.begin() + return_size, new_list.end(), [](const auto & l, const auto & r) { return l > r; });

        if (k < new_list.size())
            new_list.resize(k);

        return new_list;
    }

    void write(WriteBuffer & wb) const
    {
        std::vector<Counter> new_list;
        new_list.reserve(counter_list.size());
        for (auto & counter : counter_list)
            new_list.push_back(counter);

        size_t return_size = std::min(new_list.size(), capacity());
        ::partial_sort(
            new_list.begin(), new_list.begin() + return_size, new_list.end(), [](const auto & l, const auto & r) { return l > r; });

        writeVarUInt(return_size, wb);
        for (size_t i = 0; i < return_size; i++)
            new_list[i].write(wb);

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
            auto counter = Counter();
            counter.read(rb);
            counter.hash = counter_map.hash(counter.key);
            push(std::move(counter));
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
    NO_INLINE void push(Counter && counter)
    {
        size_t pos = counter_list.size();
        counter_map.insertIfNotPresent(counter.key, counter.hash, pos);
        counter_list.push_back(std::move(counter));
        truncateIfNeeded();
    }

    ALWAYS_INLINE void truncateIfNeeded()
    {
        if (unlikely(counter_list.size() >= capacity() * 2))
        {
            ::partial_sort(
                counter_list.begin(),
                counter_list.begin() + capacity(),
                counter_list.end(),
                [](const auto & l, const auto & r) { return l > r; });

            const size_t alpha_mask = alpha_map.size() - 1;
            for (size_t i = m_capacity; i < counter_list.size(); ++i)
            {
                arena.free(counter_list[i].key);
                alpha_map[counter_list[i].hash & alpha_mask]
                    = std::max(alpha_map[counter_list[i].hash & alpha_mask], counter_list[i].count);
            }

            counter_list.resize(m_capacity);

            /// Rebuild the counter map
            counter_map.clear();
            for (size_t i = 0; i < counter_list.size(); ++i)
            {
                counter_map.insertIfNotPresent(counter_list[i].key, counter_list[i].hash, i);
            }
        }
    }

private:
    void destroyElements()
    {
        for (auto & counter : counter_list)
            arena.free(counter.key);

        counter_map.clear();
        counter_list.clear();
        alpha_map.clear();
    }

    ALWAYS_INLINE Counter * findCounter(const TKey & key, size_t hash)
    {
        auto it = counter_map.find(key, hash);
        if (!it)
            return nullptr;

        return &counter_list[it->getMapped()];
    }


    using CounterMap = HashMapWithStackMemory<TKey, size_t, Hash, 4>;

    CounterMap counter_map;
    std::vector<Counter, AllocatorWithMemoryTracking<Counter>> counter_list;
    std::vector<UInt64, AllocatorWithMemoryTracking<UInt64>> alpha_map;
    SpaceSavingArena<TKey> arena;
    size_t m_capacity;
};

}
