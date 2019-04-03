#pragma once

#include <Core/Types.h>
#include <vector>


namespace DB
{

/// Bloom filter for strings.
class StringBloomFilter
{
public:
    using UnderType = UInt64;
    using Container = std::vector<UnderType>;

    /// size -- size of filter in bytes.
    /// hashes -- number of used hash functions.
    /// seed -- random seed for hash functions generation.
    StringBloomFilter(size_t size_, size_t hashes_, size_t seed_);
    StringBloomFilter(const StringBloomFilter & bloom_filter);

    bool find(const char * data, size_t len);
    void add(const char * data, size_t len);
    void clear();

    /// Checks if this contains everything from another bloom filter.
    /// Bloom filters must have equal size and seed.
    bool contains(const StringBloomFilter & bf);

    const Container & getFilter() const { return filter; }
    Container & getFilter() { return filter; }

    /// For debug.
    UInt64 isEmpty() const;

    friend bool operator== (const StringBloomFilter & a, const StringBloomFilter & b);
private:

    size_t size;
    size_t hashes;
    size_t seed;
    size_t words;
    Container filter;
};


bool operator== (const StringBloomFilter & a, const StringBloomFilter & b);

}
