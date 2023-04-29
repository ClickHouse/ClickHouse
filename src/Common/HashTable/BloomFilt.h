#pragma once

#include <functional>
#include <cmath>
//#include <MurmurHash3.h>
//#include <Functions/FunctionsHashing.h>
#include <city.h>
#include <random>
#include <Common/HashTable/HashTableAllocator.h>

template <typename T, typename Hash, typename Allocator = HashTableAllocator>
class BloomFilt : protected Hash,
                protected Allocator
{
public:
    BloomFilt(size_t max_item, double  = 0.001);

    size_t hash(const T & x) const { return Hash::operator()(x); }

    //void init(size_t max_item, double p = 0.01);

    void insert(const T & elem);

    bool lookup(const T & elem) const;

    double bitsPerItem() const;

    size_t getBufferSizeInBytes() const;

    ~BloomFilt() {
        if (array)
        {
            Allocator::free(array, size_of_filter * sizeof(bool));
            array = nullptr;
        }
    }

private:
    static constexpr UInt64 SEED_GEN_A = 845897321;
    static constexpr UInt64 SEED_GEN_B = 217728422;

    size_t seed;
    std::array<uint64_t, 2> _hash(size_t elem) const;
    uint64_t nthHash(size_t n,
                        uint64_t hashA,
                        uint64_t hashB) const;

    const double size_param = log(2.0);
    const size_t min_size = 10;
    size_t size_of_filter, number_of_hash, number_of_elem;
    bool * array = nullptr;

};

template <typename T, typename Hash, typename Allocator>
BloomFilt<T, Hash, Allocator>::BloomFilt(size_t max_item, double p) : number_of_elem(max_item) {
    std::mt19937_64 gen (std::random_device{}());

    size_t randomNumber = gen();
    seed = randomNumber;

    double lg = -log(p);
    size_of_filter = std::max(min_size, static_cast<size_t>(static_cast<double>(max_item) * lg / (size_param * size_param)));
    //array = new bool[size_of_filter];
    array = reinterpret_cast<bool *>(Allocator::alloc(size_of_filter * sizeof(bool)));
    number_of_hash = std::max(static_cast<size_t>(1), static_cast<size_t>(lg / size_param));
}

template <typename T, typename Hash, typename Allocator>
double BloomFilt<T, Hash, Allocator>::bitsPerItem() const {
    return (8.0 * size_of_filter * sizeof(bool)) / static_cast<double>(number_of_elem);
}

template <typename T, typename Hash, typename Allocator>
std::array<uint64_t, 2> BloomFilt<T, Hash, Allocator>::_hash(size_t elem) const {
    //std::array<uint64_t, 2> hashValue;
    //MurmurHash3_x64_128((&elem), sizeof(size_t), 0, hashValue.data());
    char * ptr = reinterpret_cast<char *>(&elem);
    size_t hash1 = CityHash_v1_0_2::CityHash64WithSeed(ptr, sizeof(size_t), seed);
    size_t hash2 = CityHash_v1_0_2::CityHash64WithSeed(ptr, sizeof(size_t), SEED_GEN_A * seed + SEED_GEN_B);

     return {hash1, hash2};
}

template <typename T, typename Hash, typename Allocator>
uint64_t BloomFilt<T, Hash, Allocator>::nthHash(size_t n,
                        uint64_t hashA,
                        uint64_t hashB)  const {
    return (hashA + n * hashB) % size_of_filter;
}

template <typename T, typename Hash, typename Allocator>
void BloomFilt<T, Hash, Allocator>::insert(const T & elem) {
    size_t _ha = hash(elem);
    std::array<uint64_t, 2> hashValues = _hash(_ha);
    for (size_t n = 0; n < number_of_hash; n++) {
      array[nthHash(n, hashValues[0], hashValues[1])] = true;
    }
}

template <typename T, typename Hash, typename Allocator>
bool BloomFilt<T, Hash, Allocator>::lookup(const T & elem) const {
    size_t _ha = hash(elem);
    std::array<uint64_t, 2> hashValues = _hash(_ha);

    for (size_t n = 0; n < number_of_hash; n++) {
        if (!array[nthHash(n, hashValues[0], hashValues[1])]) {
            return false;
        }
    }

    return true;
}

template <typename T, typename Hash, typename Allocator>
size_t BloomFilt<T, Hash, Allocator>::getBufferSizeInBytes() const {
    return size_of_filter * sizeof(bool);
}
