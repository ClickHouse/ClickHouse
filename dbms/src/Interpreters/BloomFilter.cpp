#include <Interpreters/BloomFilter.h>

#include <city.h>


namespace DB
{

static constexpr UInt64 SEED_GEN_A = 845897321;
static constexpr UInt64 SEED_GEN_B = 217728422;


StringBloomFilter::StringBloomFilter(size_t size_, size_t hashes_, size_t seed_)
    : size(size_), hashes(hashes_), seed(seed_), words((size + sizeof(UnderType) - 1) / sizeof(UnderType)), filter(words, 0) {}

StringBloomFilter::StringBloomFilter(const StringBloomFilter & bloom_filter)
    : size(bloom_filter.size), hashes(bloom_filter.hashes), seed(bloom_filter.seed), words(bloom_filter.words), filter(bloom_filter.filter) {}

bool StringBloomFilter::find(const char * data, size_t len)
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

void StringBloomFilter::add(const char * data, size_t len)
{
    size_t hash1 = CityHash_v1_0_2::CityHash64WithSeed(data, len, seed);
    size_t hash2 = CityHash_v1_0_2::CityHash64WithSeed(data, len, SEED_GEN_A * seed + SEED_GEN_B);

    for (size_t i = 0; i < hashes; ++i)
    {
        size_t pos = (hash1 + i * hash2 + i * i) % (8 * size);
        filter[pos / (8 * sizeof(UnderType))] |= (1ULL << (pos % (8 * sizeof(UnderType))));
    }
}

void StringBloomFilter::clear()
{
    filter.assign(words, 0);
}

bool StringBloomFilter::contains(const StringBloomFilter & bf)
{
    for (size_t i = 0; i < words; ++i)
    {
        if ((filter[i] & bf.filter[i]) != bf.filter[i])
            return false;
    }
    return true;
}

UInt64 StringBloomFilter::isEmpty() const
{
    for (size_t i = 0; i < words; ++i)
        if (filter[i] != 0)
            return false;
    return true;
}

bool operator== (const StringBloomFilter & a, const StringBloomFilter & b)
{
    for (size_t i = 0; i < a.words; ++i)
        if (a.filter[i] != b.filter[i])
            return false;
    return true;
}

}
