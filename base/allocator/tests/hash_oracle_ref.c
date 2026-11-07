/// Exposes jemalloc's static inline MurmurHash3 functions (`hash.h`) to `hash_oracle.cpp`.

#include "jemalloc/internal/hash.h"
#include "jemalloc/internal/jemalloc_preamble.h"

uint32_t ref_hash_x86_32(const void * key, int len, uint32_t seed)
{
    return hash_x86_32(key, len, seed);
}

void ref_hash_x86_128(const void * key, int len, uint32_t seed, uint64_t result_out[2])
{
    hash_x86_128(key, len, seed, result_out);
}

void ref_hash_x64_128(const void * key, int len, uint32_t seed, uint64_t result_out[2])
{
    hash_x64_128(key, len, seed, result_out);
}

void ref_hash(const void * key, size_t len, uint32_t seed, size_t result_hash[2])
{
    hash(key, len, seed, result_hash);
}
