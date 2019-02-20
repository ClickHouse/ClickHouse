#pragma once

#include <Core/Types.h>
#include <vector>

namespace DB
{

/// Good constants for LCG from wikipedia.
constexpr UInt64 LCG_A = 84589;
constexpr UInt64 LCG_C = 45989;
constexpr UInt64 LCG_M = 217728;

/// LinearCongruentialGenerator for generating random seeds for hash functions.
/// It is used there because it is very fast and lightweight.
/// https://en.wikipedia.org/wiki/Linear_congruential_generator
class LinearCongruentialGenerator
{
public:
    LinearCongruentialGenerator(
            size_t seed, UInt64 a_ = LCG_A, UInt64 c_ = LCG_C, UInt64 m_ = LCG_M);

    UInt64 next();

private:
    UInt64 current;
    UInt64 a;
    UInt64 c;
    UInt64 m;
};


/// Bloom filter for strings.
class StringBloomFilter
{
public:
    /// size -- size of filter in bytes.
    /// hashes -- number of used hash functions.
    /// seed -- random seed for hash functions generation.
    StringBloomFilter(size_t size_, size_t hashes_, size_t seed_);

    bool find(const char * data, size_t len);
    void add(const char * data, size_t len);

    /// Checks if this contains everything from another bloom filter.
    /// Bloom filters must have equal size and seed.
    bool contains(const StringBloomFilter & bf);

    const std::vector<UInt8> & getFilter() const { return filter; };
    void setFilter(std::vector<UInt8> && new_filter) { filter = std::move(new_filter); };

private:
    size_t size;
    size_t hashes;
    size_t seed;
    std::vector<UInt8> filter;
};

}
