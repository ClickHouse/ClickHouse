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

class ChaoticFilter
{

public:
    // vector<bool> is bitset with given size, vector<bool> works a little bit faster than vector<ui64>.
    using UnderType = char;
    using Container = std::vector<UnderType>;
    using HashType = UInt64;

    /// size -- size of filter in bytes.
    /// hashes -- number of used hash functions.
    /// seed -- random seed for hash functions generation.
    ChaoticFilter(size_t size_)
        : filter(size_, 0), size(size_)
    {
    }

    bool contains(HashType hash) const
    {
        UnderType proof = getBits<120 % 64, 127 % 64, UnderType>(hash);
        if (proof == 0) proof = 1;

        UInt64 hash1 = getBits<0, 29, UnderType>(hash) % size;
        if (filter[hash1] == proof) { return true; }
        UInt64 hash2 = getBits<30, 59, UnderType>(hash) % size;
        if (filter[hash2] == proof) { return true; }
        UInt64 hash3 = (getBits<60, 63, UnderType>(hash) + (getBits<64 % 64, 89 % 64, UnderType>(hash) << 4)) % size;
        if (filter[hash3] == proof) { return true; }
        UInt64 hash4 = getBits<90 % 64, 119 % 64, UnderType>(hash) % size;
        if (filter[hash4] == proof) { return true; }

        return false;
    }
    void add(HashType hash)
    {
        ++elements_added;
        UnderType proof = static_cast<UnderType>(getBits<120 % 64, 127 % 64>(hash));
        if (proof == 0) proof = 1;

        UInt64 hash1 = getBits<0, 29>(hash) % size;
        if (filter[hash1] == proof) { return; }
        if (filter[hash1] == 0) { filter[hash1] = proof; return; }
        UInt64 hash2 = getBits<30, 59>(hash) % size;
        if (filter[hash2] == proof) { return; }
        if (filter[hash2] == 0) { filter[hash2] = proof; return; }
        UInt64 hash3 = (getBits<60, 63>(hash) + (getBits<64 % 64, 89 % 64>(hash) << 4)) % size;
        if (filter[hash3] == proof) { return; }
        if (filter[hash3] == 0) { filter[hash3] = proof; return; }
        UInt64 hash4 = getBits<90 % 64, 119 % 64>(hash) % size;
        if (filter[hash4] == proof) { return; }
        if (filter[hash4] == 0) { filter[hash4] = proof; return; }
    }

    size_t added_counts() const { return elements_added; }
private:

    Container filter;

    const size_t size;

    size_t elements_added = 0;

public:
    template<size_t L, size_t R, typename returnType = UInt64>
    returnType getBits(UInt64 x) const {
        if constexpr (R == 63) {
            return x >> L;
        } else {
            return (((1ull << (R + 1)) - (1ull << L)) & x) >> L;
        }
    }

};

using ChaoticFilterPtr = std::shared_ptr<ChaoticFilter>;

}
