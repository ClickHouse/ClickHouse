#include <allocator/Hash.h>

#include "Test.h"

#include <cstring>

using namespace jemalloc;

namespace
{

enum class Variant
{
    X86_32,
    X86_128,
    X64_128,
};

/// The verification procedure of jemalloc's `test/unit/hash.c` (from SMHasher).
uint32_t verify(Variant variant, uint8_t * key)
{
    const int hashbytes = variant == Variant::X86_32 ? 4 : 16;
    const int hashes_size = hashbytes * 256;
    uint8_t hashes[16 * 256];
    uint8_t final_hash[16];

    std::memset(key, 0, 256);
    std::memset(hashes, 0, sizeof(hashes));
    std::memset(final_hash, 0, sizeof(final_hash));

    for (unsigned i = 0; i < 256; ++i)
    {
        key[i] = uint8_t(i);
        switch (variant)
        {
            case Variant::X86_32:
            {
                uint32_t out = hash::x86_32(key, int(i), 256 - i);
                std::memcpy(&hashes[i * hashbytes], &out, size_t(hashbytes));
                break;
            }
            case Variant::X86_128:
            {
                uint64_t out[2];
                hash::x86_128(key, int(i), 256 - i, out);
                std::memcpy(&hashes[i * hashbytes], out, size_t(hashbytes));
                break;
            }
            case Variant::X64_128:
            {
                uint64_t out[2];
                hash::x64_128(key, int(i), 256 - i, out);
                std::memcpy(&hashes[i * hashbytes], out, size_t(hashbytes));
                break;
            }
        }
    }

    switch (variant)
    {
        case Variant::X86_32:
        {
            uint32_t out = hash::x86_32(hashes, hashes_size, 0);
            std::memcpy(final_hash, &out, sizeof(out));
            break;
        }
        case Variant::X86_128:
        {
            uint64_t out[2];
            hash::x86_128(hashes, hashes_size, 0, out);
            std::memcpy(final_hash, out, sizeof(out));
            break;
        }
        case Variant::X64_128:
        {
            uint64_t out[2];
            hash::x64_128(hashes, hashes_size, 0, out);
            std::memcpy(final_hash, out, sizeof(out));
            break;
        }
    }

    return uint32_t(final_hash[0]) | (uint32_t(final_hash[1]) << 8) | (uint32_t(final_hash[2]) << 16) | (uint32_t(final_hash[3]) << 24);
}

}

TEST(Hash, Verification)
{
    const uint32_t expected_x86_32 = config::big_endian ? 0x6213303eU : 0xb0f57ee3U;
    const uint32_t expected_x86_128 = config::big_endian ? 0x266820caU : 0xb3ece62aU;
    const uint32_t expected_x64_128 = config::big_endian ? 0xcc622b6fU : 0x6384ba69U;

    /// All alignments of the key.
    uint8_t key[256 + 15];
    for (size_t i = 0; i < 16; ++i)
    {
        CHECK_EQ(verify(Variant::X86_32, &key[i]), expected_x86_32);
        CHECK_EQ(verify(Variant::X86_128, &key[i]), expected_x86_128);
        CHECK_EQ(verify(Variant::X64_128, &key[i]), expected_x64_128);
    }
}

TEST(Hash, Dispatch)
{
    const char * s = "jemalloc";
    size_t r[2];
    hash::hash(s, std::strlen(s), 0x94122f33U, r);
    uint64_t expected[2];
    if constexpr (!config::big_endian)
        hash::x64_128(s, int(std::strlen(s)), 0x94122f33U, expected);
    else
        hash::x86_128(s, int(std::strlen(s)), 0x94122f33U, expected);
    CHECK_EQ(r[0], expected[0]);
    CHECK_EQ(r[1], expected[1]);
}
