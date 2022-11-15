#pragma once

#include <vector>
#include <base/types.h>
#include <Core/Field.h>
#include <Common/PODArray.h>
#include <Common/Allocator.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/IDataType.h>


namespace DB
{
struct BloomFilterParameters
{
    BloomFilterParameters(size_t filter_size_, size_t filter_hashes_, size_t seed_);

    /// size of filter in bytes.
    size_t filter_size;
    /// number of used hash functions.
    size_t filter_hashes;
    /// random seed for hash functions generation.
    size_t seed;
};

class BloomFilter
{

public:
    using UnderType = UInt64;
    using Container = std::vector<UnderType>;

    explicit BloomFilter(const BloomFilterParameters & params);
    /// size -- size of filter in bytes.
    /// hashes -- number of used hash functions.
    /// seed -- random seed for hash functions generation.
    BloomFilter(size_t size_, size_t hashes_, size_t seed_);

    bool find(const char * data, size_t len);
    void add(const char * data, size_t len);
    void clear();

    void addHashWithSeed(const UInt64 & hash, const UInt64 & hash_seed);
    bool findHashWithSeed(const UInt64 & hash, const UInt64 & hash_seed);

    /// Checks if this contains everything from another bloom filter.
    /// Bloom filters must have equal size and seed.
    bool contains(const BloomFilter & bf);

    const Container & getFilter() const { return filter; }
    Container & getFilter() { return filter; }

    /// For debug.
    UInt64 isEmpty() const;

    friend bool operator== (const BloomFilter & a, const BloomFilter & b);
private:

    size_t size;
    size_t hashes;
    size_t seed;
    size_t words;
    Container filter;

public:
    static ColumnPtr getPrimitiveColumn(const ColumnPtr & column);
    static DataTypePtr getPrimitiveType(const DataTypePtr & data_type);
};

using BloomFilterPtr = std::shared_ptr<BloomFilter>;

bool operator== (const BloomFilter & a, const BloomFilter & b);


}
