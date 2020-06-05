#pragma once

#include <vector>
#include <Core/Types.h>
#include <Core/Field.h>
#include <Common/PODArray.h>
#include <Common/Allocator.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/IDataType.h>

namespace DB
{

class BasicBloomFilter
{

public:
    // vector<bool> is bitset with given size, vector<bool> works a little bit faster than vector<ui64>.
    using UnderType = bool;
    using Container = std::vector<UnderType>;
    using HashType = UInt64;

    /// size -- size of filter in bytes.
    /// hashes -- number of used hash functions.
    /// seed -- random seed for hash functions generation.
    BasicBloomFilter(size_t size_, size_t hashes_)
        : filter(size_ * 8, 0),
          size(size_ * 8), hashes(hashes_)

    {
    }

    bool contains(HashType hash) const
    {
        for (size_t i = 0; i < hashes; ++i)
        {
            if (!getBit((hash + (hash >> 32) * i) % size))
            {
                return false;
            }
        }
        return true;
    }
    void add(HashType hash)
    {
        ++elements_added;
        for (size_t i = 0; i < hashes; ++i)
        {
            addBit((hash + (hash >> 32) * i) % size);
        }
    }

    size_t added_counts() const { return elements_added; }
private:

    Container filter;

    const size_t size;
    const size_t hashes;

    size_t elements_added = 0;

public:
    void addBit(size_t pos)
    {
        filter[pos] = true;
    }

    bool getBit(size_t pos) const
    {
        return filter[pos];
    }
};

using BasicBloomFilterPtr = std::shared_ptr<BasicBloomFilter>;

}
