#pragma once

#include <Common/HashTable/Hash.h>
#include <common/logger_useful.h>
#include <type_traits>
#include <vector>

namespace DB
{

namespace
{
    inline size_t roundUpToPowerOfTwoOrZero(size_t x)
    {
        size_t r = 8;
        while (x > r)
            r <<= 1;
        return r;
    }
}

struct EmptyDeleter {};

struct Int64Hasher
{
    size_t operator()(const size_t x) const
    {
        return intHash64(x);
    }
};


/*
    Class for storing cache index.
    It consists of two arrays.
    The first one is split into buckets (each stores 8 elements (cells)) determined by hash of the element key.
    The second one is split into 4bit numbers, which are positions in bucket for next element write (So cache uses FIFO eviction algorithm inside each bucket).
*/
template <typename K, typename V, typename Hasher, typename Deleter = EmptyDeleter>
class BucketCacheIndex
{
    struct Cell
    {
        K key;
        V index;
    };

public:
    template <typename = std::enable_if<std::is_same_v<EmptyDeleter, Deleter>>>
    BucketCacheIndex(size_t cells_)
        : buckets(roundUpToPowerOfTwoOrZero(cells_) / bucket_size)
        , bucket_mask(buckets - 1)
        , cells(buckets * bucket_size)
        , positions((buckets / 2) + 1)
    {
        for (auto & cell : cells)
            cell.index.setNotExists();
        for (size_t bucket = 0; bucket < buckets; ++bucket)
            setPosition(bucket, 0);
    }

    template <typename = std::enable_if<!std::is_same_v<EmptyDeleter, Deleter>>>
    BucketCacheIndex(size_t cells_, Deleter deleter_)
        : deleter(deleter_)
        , buckets(roundUpToPowerOfTwoOrZero(cells_) / bucket_size)
        , bucket_mask(buckets - 1)
        , cells(buckets * bucket_size)
        , positions((buckets / 2) + 1)
    {
        for (auto & cell : cells)
            cell.index.setNotExists();
        for (size_t bucket = 0; bucket < buckets; ++bucket)
            setPosition(bucket, 0);
    }

    void set(K key, V val)
    {
        const size_t bucket = (hash(key) & bucket_mask);
        const size_t idx = getCellIndex(key, bucket);
        if (!cells[idx].index.exists())
        {
            incPosition(bucket);
            ++sz;
        }

        cells[idx].key = key;
        cells[idx].index = val;
    }

    template <typename = std::enable_if<!std::is_same_v<EmptyDeleter, Deleter>>>
    void setWithDelete(K key, V val)
    {
        const size_t bucket = (hash(key) & bucket_mask);
        const size_t idx = getCellIndex(key, bucket);
        if (!cells[idx].index.exists())
        {
            incPosition(bucket);
            ++sz;
        }
        else
        {
            deleter(cells[idx].key);
        }

        cells[idx].key = key;
        cells[idx].index = val;
    }

    bool get(K key, V & val) const
    {
        const size_t bucket = (hash(key) & bucket_mask);
        const size_t idx = getCellIndex(key, bucket);
        if (!cells[idx].index.exists() || cells[idx].key != key)
            return false;
        val = cells[idx].index;
        return true;
    }

    bool getKeyAndValue(K & key, V & val) const
    {
        const size_t bucket = (hash(key) & bucket_mask);
        const size_t idx = getCellIndex(key, bucket);
        if (!cells[idx].index.exists() || cells[idx].key != key)
            return false;
        key = cells[idx].key;
        val = cells[idx].index;
        return true;
    }

    bool erase(K key)
    {
        const size_t bucket = (hash(key) & bucket_mask);
        const size_t idx = getCellIndex(key, bucket);
        if (!cells[idx].index.exists() || cells[idx].key != key)
            return false;

        cells[idx].index.setNotExists();
        --sz;
        if constexpr (!std::is_same_v<EmptyDeleter, Deleter>)
            deleter(cells[idx].key);

        return true;
    }

    size_t size() const
    {
        return sz;
    }

    size_t capacity() const
    {
        return cells.size();
    }

    auto keys() const
    {
        std::vector<K> res;
        for (const auto & cell : cells)
        {
            if (cell.index.exists())
            {
                res.push_back(cell.key);
            }
        }
        return res;
    }

private:
    /// Searches for the key in the bucket.
    /// Returns index of cell with provided key.
    size_t getCellIndex(const K key, const size_t bucket) const
    {
        const size_t pos = getPosition(bucket);
        for (int idx = 7; idx >= 0; --idx)
        {
            const size_t cur = ((pos + 1 + idx) & pos_mask);
            if (cells[bucket * bucket_size + cur].index.exists() &&
                cells[bucket * bucket_size + cur].key == key)
            {
                return bucket * bucket_size + cur;
            }
        }

        return bucket * bucket_size + pos;
    }

    /// Returns current position for write in the bucket.
    size_t getPosition(const size_t bucket) const
    {
        const size_t idx = (bucket >> 1);
        if ((bucket & 1) == 0)
            return ((positions[idx] >> 4) & pos_mask);
        return (positions[idx] & pos_mask);
    }

    /// Sets current posiotion in the bucket.
    void setPosition(const size_t bucket, const size_t pos)
    {
        const size_t idx = bucket >> 1;
        if ((bucket & 1) == 0)
            positions[idx] = ((pos << 4) | (positions[idx] & ((1 << 4) - 1)));
        else
            positions[idx] = (pos | (positions[idx] & (((1 << 4) - 1) << 4)));
    }

    void incPosition(const size_t bucket)
    {
        setPosition(bucket, (getPosition(bucket) + 1) & pos_mask);
    }

    static constexpr size_t bucket_size = 8;
    static constexpr size_t pos_size = 3;
    static constexpr size_t pos_mask = (1 << pos_size) - 1;

    Hasher hash;
    Deleter deleter;

    size_t buckets;
    size_t bucket_mask;

    std::vector<Cell> cells;
    std::vector<char> positions;
    size_t sz = 0;
};

}
