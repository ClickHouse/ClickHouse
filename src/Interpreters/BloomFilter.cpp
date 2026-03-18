#include <Interpreters/BloomFilter.h>
#include <city.h>
#include <Columns/ColumnArray.h>
#include <Common/Exception.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnLowCardinality.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
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

ALWAYS_INLINE BloomFilterHashPair makeCityHashPair(const char * data, size_t size, UInt64 seed)
{
    return
    {
        CityHash_v1_0_2::CityHash64WithSeed(data, size, seed),
        CityHash_v1_0_2::CityHash64WithSeed(data, size, SEED_GEN_A * seed + SEED_GEN_B)
    };
}

ALWAYS_INLINE UInt64 hashWithSeedCityHash(UInt64 hash, UInt64 hash_seed)
{
    return CityHash_v1_0_2::Hash128to64(CityHash_v1_0_2::uint128(hash, hash_seed));
}

ALWAYS_INLINE size_t fastMod(size_t value, size_t modulus, const BloomDivider & divider)
{
    return value - (value / divider) * modulus;
}

ALWAYS_INLINE void addHashPairGeneric(
    BloomFilter::Container & filter,
    const BloomFilterHashPair & pair,
    size_t hashes,
    size_t modulus,
    const BloomDivider & divider)
{
    size_t acc = pair.hash1;
    for (size_t i = 0; i < hashes; ++i)
    {
        size_t pos = fastMod(acc + i * i, modulus, divider);
        filter[pos / BLOOM_WORD_BITS] |= (1ULL << (pos % BLOOM_WORD_BITS));
        acc += pair.hash2;
    }
}

ALWAYS_INLINE bool findHashPairGeneric(
    const BloomFilter::Container & filter,
    const BloomFilterHashPair & pair,
    size_t hashes,
    size_t modulus,
    const BloomDivider & divider)
{
    size_t acc = pair.hash1;
    for (size_t i = 0; i < hashes; ++i)
    {
        size_t pos = fastMod(acc + i * i, modulus, divider);
        if (!(filter[pos / BLOOM_WORD_BITS] & (1ULL << (pos % BLOOM_WORD_BITS))))
            return false;
        acc += pair.hash2;
    }
    return true;
}

ALWAYS_INLINE void addHashPairK3(
    BloomFilter::Container & filter,
    const BloomFilterHashPair & pair,
    size_t modulus,
    const BloomDivider & divider)
{
    size_t acc = pair.hash1;
    for (size_t i = 0; i < 3; ++i)
    {
        size_t pos = fastMod(acc + i * i, modulus, divider);
        filter[pos / BLOOM_WORD_BITS] |= (1ULL << (pos % BLOOM_WORD_BITS));
        acc += pair.hash2;
    }
}

ALWAYS_INLINE bool findHashPairK3(
    const BloomFilter::Container & filter,
    const BloomFilterHashPair & pair,
    size_t modulus,
    const BloomDivider & divider)
{
    size_t acc = pair.hash1;
    for (size_t i = 0; i < 3; ++i)
    {
        size_t pos = fastMod(acc + i * i, modulus, divider);
        if (!(filter[pos / BLOOM_WORD_BITS] & (1ULL << (pos % BLOOM_WORD_BITS))))
            return false;
        acc += pair.hash2;
    }
    return true;
}

[[maybe_unused]] static void addHashPairsK3Impl(
    BloomFilter::Container & filter,
    const BloomFilterHashPair * pairs,
    size_t count,
    size_t modulus,
    const BloomDivider & divider)
{
    for (size_t i = 0; i < count; ++i)
        addHashPairK3(filter, pairs[i], modulus, divider);
}

[[maybe_unused]] static size_t findHashPairsK3Impl(
    const BloomFilter::Container & filter,
    const BloomFilterHashPair * pairs,
    size_t count,
    size_t modulus,
    const BloomDivider & divider,
    UInt8 * out_mask)
{
    size_t found_count = 0;
    for (size_t i = 0; i < count; ++i)
    {
        const bool found = findHashPairK3(filter, pairs[i], modulus, divider);
        out_mask[i] = found;
        found_count += found;
    }
    return found_count;
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

bool BloomFilter::find(const char * data, size_t len) const
{
    return findHashPair(makeCityHashPair(data, len, seed));
}

void BloomFilter::add(const char * data, size_t len)
{
    addHashPair(makeCityHashPair(data, len, seed));
}

void BloomFilter::addHashPair(const BloomFilterHashPair & pair)
{
    if (hashes == 3)
    {
        addHashPairK3(filter, pair, modulus, divider);
        return;
    }
    addHashPairGeneric(filter, pair, hashes, modulus, divider);
}

bool BloomFilter::findHashPair(const BloomFilterHashPair & pair) const
{
    if (hashes == 3)
        return findHashPairK3(filter, pair, modulus, divider);
    return findHashPairGeneric(filter, pair, hashes, modulus, divider);
}

void BloomFilter::addHashPairs(const BloomFilterHashPair * pairs, size_t count)
{
    if (count == 0)
        return;

    if (hashes == 3)
    {
        addHashPairsK3Impl(filter, pairs, count, modulus, divider);
        return;
    }

    for (size_t i = 0; i < count; ++i)
        addHashPairGeneric(filter, pairs[i], hashes, modulus, divider);
}

size_t BloomFilter::findHashPairs(const BloomFilterHashPair * pairs, size_t count, UInt8 * out_mask) const
{
    if (count == 0)
        return 0;

    if (hashes == 3)
        return findHashPairsK3Impl(filter, pairs, count, modulus, divider, out_mask);

    size_t found_count = 0;
    for (size_t i = 0; i < count; ++i)
    {
        const bool found = findHashPairGeneric(filter, pairs[i], hashes, modulus, divider);
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
