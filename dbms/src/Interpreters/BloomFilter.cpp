#include <Interpreters/BloomFilter.h>

#include <city.h>


namespace DB
{

StringBloomFilter::StringBloomFilter(size_t size_, size_t hashes_, size_t seed_)
    : size(size_), hashes(hashes_), seed(seed_), filter(size, 0) {}

bool StringBloomFilter::find(const char * data, size_t len)
{
    LinearCongruentialGenerator lcg(seed);
    for (size_t i = 0; i < hashes; ++i)
    {
        size_t pos = CityHash_v1_0_2::CityHash64WithSeed(data, len, lcg()) % (8 * size);
        if (!(filter[pos / 8] & (1 << (pos % 8))))
            return false;
    }
    return true;
}

void StringBloomFilter::add(const char * data, size_t len)
{
    LinearCongruentialGenerator lcg(seed);
    for (size_t i = 0; i < hashes; ++i)
    {
        size_t pos = CityHash_v1_0_2::CityHash64WithSeed(data, len, lcg()) % (8 * size);
        filter[pos / 8] |= (1 << (pos % 8));
    }
}

void StringBloomFilter::clear()
{
    filter.assign(size, 0);
}

bool StringBloomFilter::contains(const StringBloomFilter & bf)
{
    for (size_t i = 0; i < size; ++i)
    {
        if ((filter[i] & bf.filter[i]) != bf.filter[i])
            return false;
    }
    return true;
}

void StringBloomFilter::merge(const StringBloomFilter & bf)
{
    for (size_t i = 0; i < size; ++i)
        filter[i] |= bf.filter[i];
}

UInt64 StringBloomFilter::getFingerPrint() const
{
    return CityHash_v1_0_2::CityHash64(reinterpret_cast<const char *>(filter.data()), size);
}

bool operator== (const StringBloomFilter & a, const StringBloomFilter & b)
{
    for (size_t i = 0; i < a.size; ++i)
        if (a.filter[i] != b.filter[i])
            return false;
    return true;
}

}