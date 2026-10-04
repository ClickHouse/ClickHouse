/// Compares `hash::*` with the MurmurHash3 functions of jemalloc's `hash.h` (compiled in `hash_oracle_ref.c`).

#include <allocator/Hash.h>

#include "Test.h"

#include <random>

extern "C"
{
uint32_t ref_hash_x86_32(const void * key, int len, uint32_t seed);
void ref_hash_x86_128(const void * key, int len, uint32_t seed, uint64_t r_out[2]);
void ref_hash_x64_128(const void * key, int len, uint32_t seed, uint64_t r_out[2]);
void ref_hash(const void * key, size_t len, uint32_t seed, size_t r_hash[2]);
}

using namespace jemalloc;

TEST(HashOracle, AllVariants)
{
    std::mt19937_64 rng(2026);
    uint8_t data[1024 + 16];
    for (auto & byte : data)
        byte = static_cast<uint8_t>(rng());

    size_t compared = 0;
    for (int len = 0; len <= 1024; ++len)
    {
        for (size_t offset = 0; offset < 16; ++offset)
        {
            const uint8_t * key = data + offset;
            uint32_t seed = static_cast<uint32_t>(rng());
            if (len % 7 == 0)
                seed = (len % 2) ? 0x94122f33U : 0xd983396eU; /// The seeds used by prof and ckh.

            CHECK_EQ(hash::x86_32(key, len, seed), ref_hash_x86_32(key, len, seed));

            uint64_t expected[2];
            uint64_t actual[2];
            ref_hash_x86_128(key, len, seed, expected);
            hash::x86_128(key, len, seed, actual);
            CHECK_EQ(actual[0], expected[0]);
            CHECK_EQ(actual[1], expected[1]);

            ref_hash_x64_128(key, len, seed, expected);
            hash::x64_128(key, len, seed, actual);
            CHECK_EQ(actual[0], expected[0]);
            CHECK_EQ(actual[1], expected[1]);

            size_t expected_hash[2];
            size_t actual_hash[2];
            ref_hash(key, size_t(len), seed, expected_hash);
            hash::hash(key, size_t(len), seed, actual_hash);
            CHECK_EQ(actual_hash[0], expected_hash[0]);
            CHECK_EQ(actual_hash[1], expected_hash[1]);
            ++compared;
        }
    }
    CHECK_EQ(compared, size_t(1025 * 16));
}
