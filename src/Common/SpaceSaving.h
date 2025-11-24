#pragma once

#include <IO/VarInt.h>
#include <base/sort.h>
#include <Common/AllocatorWithMemoryTracking.h>
#include <Common/ArenaUtils.h>
#include <Common/ArenaWithFreeLists.h>
#include <Common/HashTable/ClearableHashMap.h>
#include <Common/HashTable/Hash.h>

#include <vector>


/*
 * Implementation of the Filtered Space-Saving for TopK streaming analysis.
 *   http://www.l2f.inesc-id.pt/~fmmb/wiki/uploads/Work/misnis.ref0a.pdf
 * It implements suggested reduce-and-combine algorithm from Parallel Space Saving:
 *   https://arxiv.org/pdf/1401.0702.pdf
 */

namespace DB
{

namespace ErrorCodes
{
extern const int SIZES_OF_ARRAYS_DONT_MATCH;
extern const int TOO_LARGE_ARRAY_SIZE;
}

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

    /// Limit the max alpha value to avoid overflow with merges or topKWeighted with huge weights
    /// Better to have a less precise value than an overflow (which will lead to incorrect results)
    static constexpr UInt64 MAX_ALPHA_VALUE = UInt64{UINT32_MAX};

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

        void read(ReadBuffer & rb, SpaceSavingArena<TKey> & space_arena)
        {
            if constexpr (std::is_same_v<TKey, StringRef>)
            {
                String skey;
                readBinary(skey, rb);
                key = space_arena.emplace(skey);
            }
            else
                readBinaryLittleEndian(key, rb);
            readVarUInt(count, rb);
            readVarUInt(error, rb);
        }

        // greater() taking error into account
        bool operator> (const Counter & b) const
        {
            return ((count - error) > (b.count - b.error)) || ((count - error) == (b.count - b.error) && count > b.count);
        }

        TKey key;
        size_t hash;
        UInt64 count;
        UInt64 error;
    };

    explicit SpaceSaving(size_t c = 0)
    {
        if (c != 0)
            resize(c);
    }

    ~SpaceSaving() { destroyElements(); }

    bool empty() const { return counter_list.empty(); }

    size_t size() const
    {
        return counter_list.size();
    }

    size_t capacity() const { return requested_capacity; }

    void clear()
    {
        return destroyElements();
    }

    void resize(size_t new_capacity)
    {
        if (requested_capacity != new_capacity)
        {
            chassert(empty());
            alpha_map.resize(nextAlphaSize(new_capacity), 0);
            requested_capacity = new_capacity;
            /// If the requested capacity is really small we would end up sorting and truncating too often
            target_capacity = std::max(size_t{64}, requested_capacity * 2);
            counter_list.reserve(target_capacity);
        }
    }

    void insert(const TKey & key, UInt64 increment = 1, UInt64 error = 0)
    {
        // Increase weight of a key that already exists
        auto hash = counter_map.hash(key);

        if (auto * counter = findCounter(key, hash))
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

        const UInt64 alpha_mask = alpha_map.size() - 1;
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

        if (empty())
        {
            *this = rhs;
            return;
        }

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

        if (alpha_map.size() == rhs.alpha_map.size())
        {
            for (size_t i = 0; i < alpha_map.size(); i++)
                alpha_map[i] = std::min(alpha_map[i] + rhs.alpha_map[i], MAX_ALPHA_VALUE);
        }

        truncateIfNeeded(true);
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

        new_list.resize(return_size);

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

    void read(ReadBuffer & rb, size_t capacity)
    {
        size_t count = 0;
        readVarUInt(count, rb);
        if (count > capacity)
            throw DB::Exception(
                DB::ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Found too large when reading counters (Passed: {}. Maximum: {})", count, capacity);

        if (!count)
        {
            /// It is possible that the state was written before the set was initialized, which would add 0 Counters and the alpha map
            /// The size of the alpha map depends on the version and what was the default but we can just ignore it (no Counters mean there
            /// should be no errors in the alpha map)
            skipAlphaMap(rb);
            return;
        }

        resize(capacity);
        for (size_t i = 0; i < count; ++i)
        {
            counter_list.emplace_back();
            auto & counter = counter_list.back();
            counter.read(rb, arena);
            counter.hash = counter_map.hash(counter.key);
        }

        readAlphaMap(rb);
        truncateIfNeeded(true);
    }

    void readAlphaMap(ReadBuffer & rb)
    {
        size_t alpha_size = 0;
        readVarUInt(alpha_size, rb);

        size_t expected_capacity = alpha_map.size();
        if (alpha_size != expected_capacity)
            throw DB::Exception(
                DB::ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                "Found incorrect alpha vector size (Passed: {}. Expected: {})",
                alpha_size,
                expected_capacity);

        for (size_t i = 0; i < alpha_size; ++i)
        {
            UInt64 alpha = 0;
            readVarUInt(alpha, rb);
            alpha_map[i] = alpha;
        }
    }

    void skipAlphaMap(ReadBuffer & rb)
    {
        size_t alpha_size = 0;
        readVarUInt(alpha_size, rb);

        for (size_t i = 0; i < alpha_size; ++i)
        {
            UInt64 alpha = 0;
            readVarUInt(alpha, rb);
        }
    }

protected:
    NO_INLINE void push(Counter && counter)
    {
        size_t pos = counter_list.size();
        counter_map.insertIfNotPresent(counter.key, counter.hash, pos);
        counter_list.push_back(std::move(counter));
        truncateIfNeeded(false);
    }

    ALWAYS_INLINE void truncateIfNeeded(bool force_counter_map_rebuild)
    {
        if (unlikely(counter_list.size() >= target_capacity))
        {
            ::nth_element(
                counter_list.begin(),
                counter_list.begin() + capacity(),
                counter_list.end(),
                [](const auto & l, const auto & r) { return l > r; });

            const UInt64 alpha_mask = alpha_map.size() - 1;
            for (size_t i = requested_capacity; i < counter_list.size(); ++i)
            {
                size_t pos = counter_list[i].hash & alpha_mask;
                alpha_map[pos] = std::min(alpha_map[pos] + counter_list[i].count - counter_list[i].error, MAX_ALPHA_VALUE);
                arena.free(counter_list[i].key);
            }

            counter_list.resize(requested_capacity);
            force_counter_map_rebuild = true;
        }

        if (force_counter_map_rebuild)
        {
            /// Rebuild the counter map
            counter_map.clear();
            for (size_t i = 0; i < counter_list.size(); ++i)
                counter_map.insertIfNotPresent(counter_list[i].key, counter_list[i].hash, i);
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
        requested_capacity = 0;
    }

    ALWAYS_INLINE Counter * findCounter(const TKey & key, size_t hash)
    {
        auto it = counter_map.find(key, hash);
        if (!it)
            return nullptr;

        return &counter_list[it->getMapped()];
    }

    SpaceSaving & operator=(const SpaceSaving & rhs)
    {
        if (&rhs == this)
            return *this;

        if (!empty())
            destroyElements();
        resize(rhs.capacity());

        counter_list = rhs.counter_list;
        alpha_map = rhs.alpha_map;

        if constexpr (std::is_same_v<TKey, StringRef>)
        {
            /// Need to copy the keys into our own arena
            for (auto & counter : counter_list)
                counter.key = arena.emplace(counter.key);
        }
        truncateIfNeeded(true);

        return *this;
    }


    using CounterMap = ClearableHashMapWithStackMemoryAndSavedHash<TKey, size_t, Hash, 4>;

    CounterMap counter_map{};
    std::vector<Counter, AllocatorWithMemoryTracking<Counter>> counter_list{};
    std::vector<UInt64, AllocatorWithMemoryTracking<UInt64>> alpha_map{};
    SpaceSavingArena<TKey> arena{};
    /// Capacity requested by the user. We will never return more than this many elements in topK().
    size_t requested_capacity = 0;
    /// Internal target capacity to avoid frequent truncations.
    size_t target_capacity = 0;
};

}
