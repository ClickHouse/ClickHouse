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
        UnderType proof = static_cast<UnderType>(MurmurHash3(hash) % 256);
        if (proof == 0) proof = 1;

        UInt64 hash1 = MurmurHash3_mix1(hash) % size;
        if (filter[hash1] == proof) { return true; }

        UInt64 hash2 = MurmurHash3_mix2(hash) % size;
        if (filter[hash2] == proof) { return true; }

        UInt64 hash3 = MurmurHash3_mix3(hash) % size;
        if (filter[hash3] == proof) { return true; }

        UInt64 hash4 = MurmurHash3_mix4(hash) % size;
        if (filter[hash4] == proof) { return true; }

        return false;
    }
    void add(HashType hash)
    {
        ++elements_added;
        UnderType proof = static_cast<UnderType>(MurmurHash3(hash) % 256);
        if (proof == 0) proof = 1;

        // UInt64 hash1 = getBits<0, 29>(hash) % size;
        UInt64 hash1 = MurmurHash3_mix1(hash) % size;
        if (filter[hash1] == proof) { return; }
        if (filter[hash1] == 0) { filter[hash1] = proof; return; }
        // UInt64 hash2 = getBits<30, 59>(hash) % size;
        UInt64 hash2 = MurmurHash3_mix2(hash) % size;
        if (filter[hash2] == proof) { return; }
        if (filter[hash2] == 0) { filter[hash2] = proof; return; }
        // UInt64 hash3 = (getBits<60, 63>(hash) + (getBits<64 % 64, 89 % 64>(hash) << 4)) % size;
        UInt64 hash3 = MurmurHash3_mix3(hash) % size;
        if (filter[hash3] == proof) { return; }
        if (filter[hash3] == 0) { filter[hash3] = proof; return; }
        // UInt64 hash4 = getBits<90 % 64, 119 % 64>(hash) % size;
        UInt64 hash4 = MurmurHash3_mix4(hash) % size;
        if (filter[hash4] == proof) { return; }
        if (filter[hash4] == 0) { filter[hash4] = proof; return; }
    }

    size_t added_counts() const { return elements_added; }
private:

    Container filter;

    const size_t size;

    size_t elements_added = 0;

    inline DB::UInt64 MurmurHash3(DB::UInt64 x) const
    {
        x ^= x >> 33;
        x *= 0xff51afd7ed558ccdULL;
        x ^= x >> 33;
        x *= 0xc4ceb9fe1a85ec53ULL;
        x ^= x >> 33;
        return x;
    }

    inline DB::UInt64 MurmurHash3_mix1(DB::UInt64 x) const
    {
        x ^= x >> 31;
        x *= 0x7fb5d329728ea185ULL;
        x ^= x >> 27;
        x *= 0x81dadef4bc2dd44dULL;
        x ^= x >> 33;
        return x;
    }

    inline DB::UInt64 MurmurHash3_mix2(DB::UInt64 x) const
    {
        x ^= x >> 33;
        x *= 0x64dd81482cbd31d7ULL;
        x ^= x >> 31;
        x *= 0xe36aa5c613612997ULL;
        x ^= x >> 31;
        return x;
    }

    inline DB::UInt64 MurmurHash3_mix3(DB::UInt64 x) const
    {
        x ^= x >> 31;
        x *= 0x99bcf6822b23ca35;
        x ^= x >> 30;
        x *= 0x14020a57acced8b7;
        x ^= x >> 33;
        return x;
    }

    inline DB::UInt64 MurmurHash3_mix4(DB::UInt64 x) const
    {
        x ^= x >> 33;
        x *= 0x62a9d9ed799705f5;
        x ^= x >> 28;
        x *= 0xcb24d0a5c88c35b3;
        x ^= x >> 32;
        return x;
    }

};

using ChaoticFilterPtr = std::shared_ptr<ChaoticFilter>;

}
