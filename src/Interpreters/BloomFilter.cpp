#include <Interpreters/BloomFilter.h>
#include <city.h>
#include <Columns/ColumnArray.h>
#include <Common/Exception.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnLowCardinality.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <libdivide-config.h>
#include <libdivide.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

static constexpr UInt64 SEED_GEN_A = 845897321;
static constexpr UInt64 SEED_GEN_B = 217728422;

static constexpr UInt64 MAX_BLOOM_FILTER_SIZE = 1 << 30;

namespace
{
using BloomDivider = libdivide::divider<size_t, libdivide::BRANCHFREE>;
constexpr size_t BLOOM_WORD_BITS = 8 * sizeof(BloomFilter::UnderType);

ALWAYS_INLINE UInt64 hashWithSeedCityHash(UInt64 hash, UInt64 hash_seed)
{
    return CityHash_v1_0_2::Hash128to64(CityHash_v1_0_2::uint128(hash, hash_seed));
}

ALWAYS_INLINE size_t fastMod(size_t value, size_t modulus, const BloomDivider & divider)
{
    return value - (value / divider) * modulus;
}

/// `add` and `find` share the same loop. When `compile_time_hashes` is non-zero the number of
/// hash functions is known at compile time, so the compiler unrolls the loop; otherwise the runtime
/// `hashes` argument is used. This keeps a single source of truth for both the generic and the
/// specialized (e.g. the default `k = 3`) code paths.
template <size_t compile_time_hashes>
ALWAYS_INLINE void addHashPairToFilter(
    BloomFilter::Container & filter,
    const BloomFilterHashPair & pair,
    size_t hashes,
    size_t modulus,
    const BloomDivider & divider)
{
    const size_t count = compile_time_hashes != 0 ? compile_time_hashes : hashes;
    size_t acc = pair.hash1;
    for (size_t i = 0; i < count; ++i)
    {
        size_t pos = fastMod(acc + i * i, modulus, divider);
        filter[pos / BLOOM_WORD_BITS] |= (1ULL << (pos % BLOOM_WORD_BITS));
        acc += pair.hash2;
    }
}

template <size_t compile_time_hashes>
ALWAYS_INLINE bool findHashPairInFilter(
    const BloomFilter::Container & filter,
    const BloomFilterHashPair & pair,
    size_t hashes,
    size_t modulus,
    const BloomDivider & divider)
{
    const size_t count = compile_time_hashes != 0 ? compile_time_hashes : hashes;
    size_t acc = pair.hash1;
    for (size_t i = 0; i < count; ++i)
    {
        size_t pos = fastMod(acc + i * i, modulus, divider);
        if (!(filter[pos / BLOOM_WORD_BITS] & (1ULL << (pos % BLOOM_WORD_BITS))))
            return false;
        acc += pair.hash2;
    }
    return true;
}
}


BloomFilterParameters::BloomFilterParameters(size_t filter_size_, size_t filter_hashes_, size_t seed_)
    : filter_size(filter_size_), filter_hashes(filter_hashes_), seed(seed_)
{
    if (filter_size == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The size of bloom filter cannot be zero");
    if (filter_hashes == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The number of hash functions for bloom filter cannot be zero");
    if (filter_size > MAX_BLOOM_FILTER_SIZE)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The size of bloom filter cannot be more than {}", MAX_BLOOM_FILTER_SIZE);
}


BloomFilter::BloomFilter(const BloomFilterParameters & params)
    : BloomFilter(params.filter_size, params.filter_hashes, params.seed)
{
}

BloomFilter::BloomFilter(size_t size_, size_t hashes_, size_t seed_)
    : size(size_), hashes(hashes_), seed(seed_), words((size + sizeof(UnderType) - 1) / sizeof(UnderType)),
      modulus(8 * size_), divider(modulus), filter(words, 0)
{
    chassert(size != 0);
    chassert(hashes != 0);
}

void BloomFilter::resize(size_t size_)
{
    size = size_;
    words = ((size + sizeof(UnderType) - 1) / sizeof(UnderType));
    modulus = 8 * size;
    divider = libdivide::divider<size_t, libdivide::BRANCHFREE>(modulus);
    filter.resize(words);
}

BloomFilterHashPair BloomFilter::computeHashPair(const char * data, size_t len, UInt64 seed_)
{
    return
    {
        CityHash_v1_0_2::CityHash64WithSeed(data, len, seed_),
        CityHash_v1_0_2::CityHash64WithSeed(data, len, SEED_GEN_A * seed_ + SEED_GEN_B)
    };
}

bool BloomFilter::find(const char * data, size_t len) const
{
    return findHashPair(computeHashPair(data, len, seed));
}

void BloomFilter::add(const char * data, size_t len)
{
    addHashPair(computeHashPair(data, len, seed));
}

void BloomFilter::addHashPair(const BloomFilterHashPair & pair)
{
    if (hashes == 3)
        addHashPairToFilter<3>(filter, pair, hashes, modulus, divider);
    else
        addHashPairToFilter<0>(filter, pair, hashes, modulus, divider);
}

bool BloomFilter::findHashPair(const BloomFilterHashPair & pair) const
{
    if (hashes == 3)
        return findHashPairInFilter<3>(filter, pair, hashes, modulus, divider);
    return findHashPairInFilter<0>(filter, pair, hashes, modulus, divider);
}

void BloomFilter::addHashPairs(const BloomFilterHashPair * pairs, size_t count)
{
    if (count == 0)
        return;

    /// Dispatch on the number of hash functions once, outside the loop, so the specialized path stays unrolled.
    if (hashes == 3)
    {
        for (size_t i = 0; i < count; ++i)
            addHashPairToFilter<3>(filter, pairs[i], hashes, modulus, divider);
        return;
    }

    for (size_t i = 0; i < count; ++i)
        addHashPairToFilter<0>(filter, pairs[i], hashes, modulus, divider);
}

size_t BloomFilter::findHashPairs(const BloomFilterHashPair * pairs, size_t count, UInt8 * out_mask) const
{
    if (count == 0)
        return 0;

    size_t found_count = 0;

    /// Dispatch on the number of hash functions once, outside the loop, so the specialized path stays unrolled.
    if (hashes == 3)
    {
        for (size_t i = 0; i < count; ++i)
        {
            const bool found = findHashPairInFilter<3>(filter, pairs[i], hashes, modulus, divider);
            out_mask[i] = found;
            found_count += found;
        }
        return found_count;
    }

    for (size_t i = 0; i < count; ++i)
    {
        const bool found = findHashPairInFilter<0>(filter, pairs[i], hashes, modulus, divider);
        out_mask[i] = found;
        found_count += found;
    }
    return found_count;
}

void BloomFilter::clear()
{
    filter.assign(words, 0);
}

bool BloomFilter::contains(const BloomFilter & bf)
{
    for (size_t i = 0; i < words; ++i)
    {
        if ((filter[i] & bf.filter[i]) != bf.filter[i])
            return false;
    }
    return true;
}

UInt64 BloomFilter::isEmpty() const
{
    for (size_t i = 0; i < words; ++i)
        if (filter[i] != 0)
            return false;
    return true;
}

size_t BloomFilter::memoryUsageBytes() const
{
    return filter.capacity() * sizeof(UnderType);
}

bool operator== (const BloomFilter & a, const BloomFilter & b)
{
    for (size_t i = 0; i < a.words; ++i)
        if (a.filter[i] != b.filter[i])
            return false;
    return true;
}

void BloomFilter::addHashWithSeed(const UInt64 & hash, const UInt64 & hash_seed)
{
    size_t pos = fastMod(hashWithSeedCityHash(hash, hash_seed));
    filter[pos / word_bits] |= (1ULL << (pos % word_bits));
}

bool BloomFilter::findHashWithSeed(const UInt64 & hash, const UInt64 & hash_seed) const
{
    size_t pos = fastMod(hashWithSeedCityHash(hash, hash_seed));
    return bool(filter[pos / word_bits] & (1ULL << (pos % word_bits)));
}

DataTypePtr BloomFilter::getPrimitiveType(const DataTypePtr & data_type)
{
    if (const auto * array_type = typeid_cast<const DataTypeArray *>(data_type.get()))
    {
        if (!typeid_cast<const DataTypeArray *>(array_type->getNestedType().get()))
            return getPrimitiveType(array_type->getNestedType());
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected type {} of bloom filter index.", data_type->getName());
    }

    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(data_type.get()))
        return getPrimitiveType(nullable_type->getNestedType());

    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(data_type.get()))
        return getPrimitiveType(low_cardinality_type->getDictionaryType());

    return data_type;
}

ColumnPtr BloomFilter::getPrimitiveColumn(const ColumnPtr & column)
{
    if (const auto * array_col = typeid_cast<const ColumnArray *>(column.get()))
        return getPrimitiveColumn(array_col->getDataPtr());

    if (const auto * nullable_col = typeid_cast<const ColumnNullable *>(column.get()))
        return getPrimitiveColumn(nullable_col->getNestedColumnPtr());

    if (const auto * low_cardinality_col = typeid_cast<const ColumnLowCardinality *>(column.get()))
        return getPrimitiveColumn(low_cardinality_col->convertToFullColumnIfLowCardinality());

    return column;
}

}
