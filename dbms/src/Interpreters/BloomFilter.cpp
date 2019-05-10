#include <Interpreters/BloomFilter.h>
#include <city.h>
#include "BloomFilter.h"


namespace DB
{

static constexpr UInt64 SEED_GEN_A = 845897321;
static constexpr UInt64 SEED_GEN_B = 217728422;

BloomFilter::BloomFilter(size_t size_, size_t hashes_, size_t seed_)
    : size(size_), hashes(hashes_), seed(seed_), words((size + sizeof(UnderType) - 1) / sizeof(UnderType)), filter(words, 0) {}

BloomFilter::BloomFilter(const BloomFilter & bloom_filter)
    : size(bloom_filter.size), hashes(bloom_filter.hashes), seed(bloom_filter.seed), words(bloom_filter.words), filter(bloom_filter.filter) {}

bool BloomFilter::find(const char * data, size_t len)
{
    size_t hash1 = CityHash_v1_0_2::CityHash64WithSeed(data, len, seed);
    size_t hash2 = CityHash_v1_0_2::CityHash64WithSeed(data, len, SEED_GEN_A * seed + SEED_GEN_B);

    for (size_t i = 0; i < hashes; ++i)
    {
        size_t pos = (hash1 + i * hash2 + i * i) % (8 * size);
        if (!(filter[pos / (8 * sizeof(UnderType))] & (1ULL << (pos % (8 * sizeof(UnderType))))))
            return false;
    }
    return true;
}

void BloomFilter::add(const char * data, size_t len)
{
    size_t hash1 = CityHash_v1_0_2::CityHash64WithSeed(data, len, seed);
    size_t hash2 = CityHash_v1_0_2::CityHash64WithSeed(data, len, SEED_GEN_A * seed + SEED_GEN_B);

    for (size_t i = 0; i < hashes; ++i)
    {
        size_t pos = (hash1 + i * hash2 + i * i) % (8 * size);
        filter[pos / (8 * sizeof(UnderType))] |= (1ULL << (pos % (8 * sizeof(UnderType))));
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

bool operator== (const BloomFilter & a, const BloomFilter & b)
{
    for (size_t i = 0; i < a.words; ++i)
        if (a.filter[i] != b.filter[i])
            return false;
    return true;
}

void BloomFilter::addHashWithSeed(const UInt64 & hash, const UInt64 & seed)
{
    size_t pos = CityHash_v1_0_2::Hash128to64(CityHash_v1_0_2::uint128(hash, seed)) % (8 * size);
    filter[pos / (8 * sizeof(UnderType))] |= (1ULL << (pos % (8 * sizeof(UnderType))));
}

bool BloomFilter::containsWithSeed(const UInt64 & hash, const UInt64 & seed)
{
    size_t pos = CityHash_v1_0_2::Hash128to64(CityHash_v1_0_2::uint128(hash, seed)) % (8 * size);
    return bool(filter[pos / (8 * sizeof(UnderType))] & (1ULL << (pos % (8 * sizeof(UnderType)))));
}

static std::pair<sMergeTreeIndexFullText.cppize_t, size_t> calculationBestPracticesImpl(double max_conflict_probability)
{
    static const size_t MAX_BITS_PER_ROW = 20;
    static const size_t MAX_HASH_FUNCTION_COUNT = 15;

    /// For the smallest index per level in probability_lookup_table
    static const size_t min_probability_index_each_bits[] = {0, 0, 1, 2, 3, 3, 4, 5, 6, 6, 7, 8, 8, 9, 10, 10, 11, 12, 12, 13, 14};

    static const long double probability_lookup_table[MAX_BITS_PER_ROW + 1][MAX_HASH_FUNCTION_COUNT] =
    {
        {1.0},  /// dummy, 0 bits per row
        {1.0, 1.0},
        {1.0, 0.393,  0.400},
        {1.0, 0.283,  0.237,   0.253},
        {1.0, 0.221,  0.155,   0.147,   0.160},
        {1.0, 0.181,  0.109,   0.092,   0.092,   0.101}, // 5
        {1.0, 0.154,  0.0804,  0.0609,  0.0561,  0.0578,   0.0638},
        {1.0, 0.133,  0.0618,  0.0423,  0.0359,  0.0347,   0.0364},
        {1.0, 0.118,  0.0489,  0.0306,  0.024,   0.0217,   0.0216,   0.0229},
        {1.0, 0.105,  0.0397,  0.0228,  0.0166,  0.0141,   0.0133,   0.0135,   0.0145},
        {1.0, 0.0952, 0.0329,  0.0174,  0.0118,  0.00943,  0.00844,  0.00819,  0.00846}, // 10
        {1.0, 0.0869, 0.0276,  0.0136,  0.00864, 0.0065,   0.00552,  0.00513,  0.00509},
        {1.0, 0.08,   0.0236,  0.0108,  0.00646, 0.00459,  0.00371,  0.00329,  0.00314},
        {1.0, 0.074,  0.0203,  0.00875, 0.00492, 0.00332,  0.00255,  0.00217,  0.00199,  0.00194},
        {1.0, 0.0689, 0.0177,  0.00718, 0.00381, 0.00244,  0.00179,  0.00146,  0.00129,  0.00121,  0.0012},
        {1.0, 0.0645, 0.0156,  0.00596, 0.003,   0.00183,  0.00128,  0.001,    0.000852, 0.000775, 0.000744}, // 15
        {1.0, 0.0606, 0.0138,  0.005,   0.00239, 0.00139,  0.000935, 0.000702, 0.000574, 0.000505, 0.00047,  0.000459},
        {1.0, 0.0571, 0.0123,  0.00423, 0.00193, 0.00107,  0.000692, 0.000499, 0.000394, 0.000335, 0.000302, 0.000287, 0.000284},
        {1.0, 0.054,  0.0111,  0.00362, 0.00158, 0.000839, 0.000519, 0.00036,  0.000275, 0.000226, 0.000198, 0.000183, 0.000176},
        {1.0, 0.0513, 0.00998, 0.00312, 0.0013,  0.000663, 0.000394, 0.000264, 0.000194, 0.000155, 0.000132, 0.000118, 0.000111, 0.000109},
        {1.0, 0.0488, 0.00906, 0.0027,  0.00108, 0.00053,  0.000303, 0.000196, 0.00014,  0.000108, 8.89e-05, 7.77e-05, 7.12e-05, 6.79e-05, 6.71e-05} // 20
    };

    for (size_t bits_per_row = 1; bits_per_row < MAX_BITS_PER_ROW; ++bits_per_row)
    {
        if (probability_lookup_table[bits_per_row][min_probability_index_each_bits[bits_per_row]] <= max_conflict_probability)
        {
            size_t max_size_of_hash_functions = min_probability_index_each_bits[bits_per_row];
            for (size_t size_of_hash_functions = max_size_of_hash_functions; size_of_hash_functions > 0; --size_of_hash_functions)
                if (probability_lookup_table[bits_per_row][size_of_hash_functions] > max_conflict_probability)
                {
                    std::cout << "Best bf:" <<  bits_per_row << ", " << (size_of_hash_functions + 1) << "\n";
                    return std::pair<size_t, size_t>(bits_per_row, size_of_hash_functions + 1);
                }

        }
    }

    return std::pair<size_t, size_t>(MAX_BITS_PER_ROW - 1, min_probability_index_each_bits[MAX_BITS_PER_ROW - 1]);
}

std::pair<size_t, size_t> calculationBestPractices(double max_conflict_probability)
{
    return calculationBestPracticesImpl(max_conflict_probability);
}

}
