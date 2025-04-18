#include <Interpreters/BloomFilter.h>
#include <city.h>
#include <Columns/ColumnArray.h>
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

bool BloomFilter::find(const char * data, size_t len)
{
    size_t hash1 = CityHash_v1_0_2::CityHash64WithSeed(data, len, seed);
    size_t hash2 = CityHash_v1_0_2::CityHash64WithSeed(data, len, SEED_GEN_A * seed + SEED_GEN_B);

    size_t acc = hash1;
    for (size_t i = 0; i < hashes; ++i)
    {
        /// It accumulates in the loop as follows:
        /// pos = (hash1 + hash2 * i + i * i) % (8 * size)
        size_t pos = fastMod(acc + i * i);
        if (!(filter[pos / word_bits] & (1ULL << (pos % word_bits))))
            return false;
        acc += hash2;
    }
    return true;
}

void BloomFilter::add(const char * data, size_t len)
{
    size_t hash1 = CityHash_v1_0_2::CityHash64WithSeed(data, len, seed);
    size_t hash2 = CityHash_v1_0_2::CityHash64WithSeed(data, len, SEED_GEN_A * seed + SEED_GEN_B);

    size_t acc = hash1;
    for (size_t i = 0; i < hashes; ++i)
    {
        /// It accumulates in the loop as follows:
        /// pos = (hash1 + hash2 * i + i * i) % (8 * size)
        size_t pos = fastMod(acc + i * i);
        filter[pos / word_bits] |= (1ULL << (pos % word_bits));
        acc += hash2;
    }
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
    size_t pos = fastMod(CityHash_v1_0_2::Hash128to64(CityHash_v1_0_2::uint128(hash, hash_seed)));
    filter[pos / word_bits] |= (1ULL << (pos % word_bits));
}

bool BloomFilter::findHashWithSeed(const UInt64 & hash, const UInt64 & hash_seed)
{
    size_t pos = fastMod(CityHash_v1_0_2::Hash128to64(CityHash_v1_0_2::uint128(hash, hash_seed)));
    return bool(filter[pos / word_bits] & (1ULL << (pos % word_bits)));
}

void BloomFilter::addRawHash(const UInt64 & hash)
{
    size_t pos = fastMod(hash);
    filter[pos / word_bits] |= (1ULL << (pos % word_bits));
}

bool BloomFilter::findRawHash(const UInt64 & hash)
{
    size_t pos = fastMod(hash);
    return bool(filter[pos / word_bits] & (1ULL << (pos % word_bits)));
}

void BloomFilter::addFarmHash(const UInt64 & hash)
{
    size_t pos = hash % (8 * size);
    filter[pos / (8 * sizeof(UnderType))] |= (1ULL << (pos % (8 * sizeof(UnderType))));
}

bool BloomFilter::findFarmHash(const UInt64 & hash)
{
    size_t pos = hash % (8 * size);
    return bool(filter[pos / (8 * sizeof(UnderType))] & (1ULL << (pos % (8 * sizeof(UnderType)))));
}

/*

bool BloomFilter::findFarmHash(const UInt64 &hash)
{
    constexpr size_t BITS_PER_UNDER_TYPE = 8 * sizeof(UnderType);
    constexpr size_t BITS_PER_BYTE = 8;
    size_t pos = hash % (BITS_PER_BYTE * size);
    size_t index = pos / BITS_PER_UNDER_TYPE;
    size_t bit_pos = pos % BITS_PER_UNDER_TYPE;

    // Bounds check to prevent out-of-bounds access
    if (index >= filter.size()) [[unlikely]]
        return false; // or handle the error appropriately

    __builtin_prefetch(&filter[index], 0, 1);

    static constexpr UInt64 BITMASKS[64] = {
        1ULL << 0, 1ULL << 1, 1ULL << 2, 1ULL << 3,
        1ULL << 4, 1ULL << 5, 1ULL << 6, 1ULL << 7,
        1ULL << 8, 1ULL << 9, 1ULL << 10, 1ULL << 11,
        1ULL << 12, 1ULL << 13, 1ULL << 14, 1ULL << 15,
        1ULL << 16, 1ULL << 17, 1ULL << 18, 1ULL << 19,
        1ULL << 20, 1ULL << 21, 1ULL << 22, 1ULL << 23,
        1ULL << 24, 1ULL << 25, 1ULL << 26, 1ULL << 27,
        1ULL << 28, 1ULL << 29, 1ULL << 30, 1ULL << 31,
        1ULL << 32, 1ULL << 33, 1ULL << 34, 1ULL << 35,
        1ULL << 36, 1ULL << 37, 1ULL << 38, 1ULL << 39,
        1ULL << 40, 1ULL << 41, 1ULL << 42, 1ULL << 43,
        1ULL << 44, 1ULL << 45, 1ULL << 46, 1ULL << 47,
        1ULL << 48, 1ULL << 49, 1ULL << 50, 1ULL << 51,
        1ULL << 52, 1ULL << 53, 1ULL << 54, 1ULL << 55,
        1ULL << 56, 1ULL << 57, 1ULL << 58, 1ULL << 59,
        1ULL << 60, 1ULL << 61, 1ULL << 62, 1ULL << 63
    };

    return (filter[index] & BITMASKS[bit_pos]) != 0;
}
*/
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
