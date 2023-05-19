#pragma once

#include <functional>
#include <cmath>
#include <algorithm>
//#include <MurmurHash3.h>
#include <city.h>
#include <cstring>
#include <Common/Exception.h>
#include <math.h> 
#include <Common/HashTable/HashTableAllocator.h>

template <typename T, typename Hash = std::hash<T>, typename Allocator = HashTableAllocator>
class CuckooFilter : protected Hash,
                protected Allocator
{
//using Backet=char*;
public:

    CuckooFilter(size_t max_item_, size_t seed_ = 0, double p = 0.01);

    size_t hash(const T & x) const { return Hash::operator()(x); }

    //void init(size_t max_item, double p = 0.01);

    bool insert(const T &elem);

    bool lookup(const T &elem) const;

    double bitsPerItem() const;

    size_t getBufferSizeInBytes() const;

    ~CuckooFilter() {
        if (array) {
            delete[] array;
        }
    }

private:
    static constexpr UInt64 SEED_GEN_A = 845897321;
    static constexpr UInt64 SEED_GEN_B = 217728422;
    size_t SEED = 0;
    size_t size_of_filter, number_of_elem, cu_number_of_elem;
    char* array = nullptr;
    size_t bytes_per_elem = 3, bits_per_elem = 12;
    static constexpr size_t max_bytes_per_elem = 20;
    
    void hash_and_fingerprint(const T &elem, char *fingpr, uint64_t *hash_) const;
    
    void swap_fingerprints(char *a, char *b) const;

    uint64_t find_hash_of_fingprint(char *fingpr) const;

    char* get_elem(size_t i) const;

    size_t allocCheckOverflow(size_t buffer_size, size_t bytes_per_elem_)
    {
        size_t size = 0;
        if (common::mulOverflow(buffer_size, bytes_per_elem_, size))
            throw DB::Exception(
                DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY,
                "Integer overflow trying to allocate memory for ProbHashTable. Trying to allocate {} cells of {} bytes each",
                buffer_size, bytes_per_elem_);

        return size;
    }
    

};

template <typename T, typename Hash, typename Allocator>
CuckooFilter<T, Hash, Allocator>::CuckooFilter(size_t max_item_, size_t seed_, double p) : SEED(seed_), number_of_elem(max_item_) {

    size_t sz = number_of_elem * 2;
    size_of_filter = 1;
    while (size_of_filter < sz) {
        size_of_filter = (size_of_filter << 1);
    }
    bytes_per_elem = std::max(static_cast<size_t>(1), static_cast<size_t>(ceil((log(1/p) + 2) / 4)));
    bits_per_elem = bytes_per_elem * 8;
   
    array = reinterpret_cast<char *>(Allocator::alloc(allocCheckOverflow(size_of_filter, bytes_per_elem)));
    for (size_t i = 0; i < size_of_filter * bytes_per_elem; i++) {
        array[i] = 0;
    }
}

template <typename T, typename Hash, typename Allocator>
char* CuckooFilter<T, Hash, Allocator>::get_elem(size_t i) const {
    return array + i * bytes_per_elem;
}


template <typename T, typename Hash, typename Allocator>
size_t CuckooFilter<T, Hash, Allocator>::getBufferSizeInBytes() const {
    return  size_of_filter * bytes_per_elem;
}

template <typename T, typename Hash, typename Allocator>
double CuckooFilter<T, Hash, Allocator>::bitsPerItem() const {
    return static_cast<double>(8.0 * getBufferSizeInBytes()) / static_cast<double>(number_of_elem);
}

template <typename T, typename Hash, typename Allocator>
void CuckooFilter<T, Hash, Allocator>::hash_and_fingerprint(const T &elem, char *fingpr, uint64_t *hash_) const {
    size_t _ha = hash(elem);
    char * ptr = reinterpret_cast<char *>(&_ha);
    size_t hash1 = CityHash_v1_0_2::CityHash64WithSeed(ptr, sizeof(size_t), SEED);
    size_t hash2 = CityHash_v1_0_2::CityHash64WithSeed(ptr, sizeof(size_t), SEED_GEN_A * SEED + SEED_GEN_B);

    uint64_t fingpr_uint = hash1 % (1 << bits_per_elem);
    if (!fingpr_uint) {
        fingpr_uint = 1;
    }
    memcpy(fingpr, reinterpret_cast<char*>(&fingpr_uint), bytes_per_elem); 
    *hash_ = hash2;
}

template <typename T, typename Hash, typename Allocator>
void CuckooFilter<T, Hash, Allocator>::swap_fingerprints(char *a, char *b) const {
    //char[bytes_per_elem] z;
    for(size_t i = 0; i < bytes_per_elem; i++) {
        std::swap(a[i], b[i]);
    }
}

template <typename T, typename Hash, typename Allocator>
uint64_t CuckooFilter<T, Hash, Allocator>::find_hash_of_fingprint(char *a) const {
    size_t hash = CityHash_v1_0_2::CityHash64WithSeed(a, bytes_per_elem, SEED);
    return hash;
}


template <typename T, typename Hash, typename Allocator>
bool CuckooFilter<T, Hash, Allocator>::insert(const T &elem) {
    if (lookup(elem)) {
        return true;
    }
    cu_number_of_elem++;
    char cu[max_bytes_per_elem];
    uint64_t ha_;
    hash_and_fingerprint(elem, cu, &ha_);
    
    size_t i = ha_ % size_of_filter;
    
    size_t fi = i;
    bool flag = false;
    uint64_t zer = 0;

    while (memcmp(get_elem(i), &zer, bytes_per_elem)) {
        swap_fingerprints(get_elem(i), cu);
        uint64_t hash_of_fingprint = find_hash_of_fingprint(cu);

        i = i ^ (hash_of_fingprint % size_of_filter);

        //number_of_kiks++;
        if (fi == i && flag) {
            return false;
        } 
        if (fi == i) {
            flag = true;
        }
    }
    memcpy(get_elem(i), cu, bytes_per_elem);
    return true;
}



template <typename T, typename Hash, typename Allocator>
bool CuckooFilter<T, Hash, Allocator>::lookup(const T &elem) const {
    //size_t _ha = Hash{}(elem);
    char cu[max_bytes_per_elem];
    uint64_t ha_;
    hash_and_fingerprint(elem, cu, &ha_);

    uint64_t hash_of_fingprint = find_hash_of_fingprint(cu);
    
    size_t i = ha_ % size_of_filter;
    size_t j = i ^ (hash_of_fingprint % size_of_filter);
    if (!memcmp(get_elem(i), cu, bytes_per_elem) || !memcmp(get_elem(j), cu, bytes_per_elem)) {
        return true;
    } else {
        return false;
    }

}
