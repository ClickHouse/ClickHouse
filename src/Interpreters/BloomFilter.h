#pragma once

#include <base/types.h>
#include <Columns/IColumn_fwd.h>
#include <DataTypes/IDataType.h>
#include <libdivide.h>

#include <vector>


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

    void resize(size_t size_);
    bool find(const char * data, size_t len) const;
    void add(const char * data, size_t len);
    void clear();

    void addHashWithSeed(const UInt64 & hash, const UInt64 & hash_seed);
    bool findHashWithSeed(const UInt64 & hash, const UInt64 & hash_seed) const;

    /// Checks if this contains everything from another bloom filter.
    /// Bloom filters must have equal size and seed.
    bool contains(const BloomFilter & bf);

    const Container & getFilter() const { return filter; }
    Container & getFilter() { return filter; }
    size_t getFilterSizeBytes() const { return size; }

    /// For debug.
    UInt64 isEmpty() const;

    size_t memoryUsageBytes() const;

    friend bool operator== (const BloomFilter & a, const BloomFilter & b);
private:

    static constexpr size_t word_bits = 8 * sizeof(UnderType);

    size_t size;
    size_t hashes;
    size_t seed;
    size_t words;
    size_t modulus; /// 8 * size, cached for fast modulo.
    libdivide::divider<size_t, libdivide::BRANCHFREE> divider; /// Divider for fast modulo by modulus.
    Container filter;

    inline size_t fastMod(size_t value) const { return value - (value / divider) * modulus; }

public:
    static ColumnPtr getPrimitiveColumn(const ColumnPtr & column);
    static DataTypePtr getPrimitiveType(const DataTypePtr & data_type);
};

using BloomFilterPtr = std::shared_ptr<BloomFilter>;

bool operator== (const BloomFilter & a, const BloomFilter & b);

}
