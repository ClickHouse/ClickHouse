#pragma once

#include <bit>
#include <string>
#include <type_traits>
#include <Core/Defines.h>
#include <base/extended_types.h>
#include <base/hex.h>
#include <base/types.h>
#include <base/unaligned.h>
#include <boost/multiprecision/cpp_int.hpp>
#include <Common/Exception.h>
#include <Common/transformEndianness.h>

#include <city.h>


class RIPEMD160
{
private:
    UInt8 digest_bytes[20];

    static constexpr UInt32 initial_digest[5] = {0x67452301UL, 0xefcdab89UL, 0x98badcfeUL, 0x10325476UL, 0xc3d2e1f0UL};

    static constexpr UInt8 rho[16] = {0x7, 0x4, 0xd, 0x1, 0xa, 0x6, 0xf, 0x3, 0xc, 0x0, 0x9, 0x5, 0x2, 0xe, 0xb, 0x8};

    static constexpr UInt8 shifts[80]
        = {11, 14, 15, 12, 5,  8,  7,  9,  11, 13, 14, 15, 6,  7,  9,  8,  12, 13, 11, 15, 6,  9,  9,  7,  12, 15, 11,
           13, 7,  8,  7,  7,  13, 15, 14, 11, 7,  7,  6,  8,  13, 14, 13, 12, 5,  5,  6,  9,  14, 11, 12, 14, 8,  6,
           5,  5,  15, 12, 15, 14, 9,  9,  8,  6,  15, 12, 13, 13, 9,  5,  8,  6,  14, 11, 12, 11, 8,  6,  5,  5};

    static constexpr UInt32 constants_left[5] = {0x00000000UL, 0x5a827999UL, 0x6ed9eba1UL, 0x8f1bbcdcUL, 0xa953fd4eUL};

   
    static ALWAYS_INLINE UInt32 make_le32(UInt32 x) noexcept
    {
        if constexpr (std::endian::native == std::endian::little)
        {
            return x;
        }
        else
        {
            return __builtin_bswap32(x);
        }
    }

    static constexpr UInt32 constants_right[5] = {0x50a28be6UL, 0x5c4dd124UL, 0x6d703ef3UL, 0x7a6d76e9UL, 0x00000000UL};

    static constexpr UInt8 fns_left[5] = {1, 2, 3, 4, 5};
    static constexpr UInt8 fns_right[5] = {5, 4, 3, 2, 1};

    static ALWAYS_INLINE UInt32 rol(UInt32 x, UInt32 n) noexcept { return (x << n) | (x >> (32 - n)); }

    static ALWAYS_INLINE UInt32 F_1(UInt32 a, UInt32 b, UInt32 c) noexcept { return (a ^ b ^ c); }

    static ALWAYS_INLINE UInt32 F_2(UInt32 a, UInt32 b, UInt32 c) noexcept { return ((a & b) | (~a & c)); }

    static ALWAYS_INLINE UInt32 F_3(UInt32 a, UInt32 b, UInt32 c) noexcept { return ((a | ~b) ^ c); }

    static ALWAYS_INLINE UInt32 F_4(UInt32 a, UInt32 b, UInt32 c) noexcept { return ((a & c) | (b & ~c)); }

    static ALWAYS_INLINE UInt32 F_5(UInt32 a, UInt32 b, UInt32 c) noexcept { return (a ^ (b | ~c)); }

    using FuncPtr = UInt32 (*)(UInt32, UInt32, UInt32);

    static constexpr FuncPtr funcs[5] = {F_1, F_2, F_3, F_4, F_5};

    static ALWAYS_INLINE FuncPtr getFunction(UInt8 func) noexcept { return funcs[func - 1]; }

    void compute_line(
        UInt32 * digest,
        UInt32 * words,
        const UInt32 * chunk,
        UInt8 * index,
        const UInt8 * sh,
        const UInt32 * ks,
        const UInt8 * fns) noexcept
    {
        std::memcpy(words, digest, 5 * sizeof(UInt32));
        for (UInt8 round = 0; round < 5; ++round)
        {
            UInt32 k = ks[round];
            UInt8 fn = fns[round];
            for (UInt8 j = 0; j < 16; ++j)
            {
                UInt32 tmp = getFunction(fn)(words[1], words[2], words[3]);
                tmp += words[0] + le32toh(chunk[index[j]]) + k;
                tmp = rol(tmp, sh[index[j]]) + words[4];
                words[0] = words[4];
                words[4] = words[3];
                words[3] = rol(words[2], 10);
                words[2] = words[1];
                words[1] = tmp;
            }
            sh += 16;
            UInt8 index_tmp[16];
            for (size_t i = 0; i < 16; ++i)
                index_tmp[i] = rho[index[i]];
            std::memcpy(index, index_tmp, 16);
        }
    }

    /// Update the digest with the given chunk of data
    void update(UInt32 * digest, const UInt32 * chunk) noexcept
    {
        UInt8 index[16];
        for (UInt8 i = 0; i < 16; ++i)
            index[i] = i;

        UInt32 words_left[5];
        compute_line(digest, words_left, chunk, index, shifts, constants_left, fns_left);

        static constexpr UInt8 rho_index[16] = {5, 14, 7, 0, 9, 2, 11, 4, 13, 6, 15, 8, 1, 10, 3, 12};
        std::memcpy(index, rho_index, 16);

        UInt32 words_right[5];
        compute_line(digest, words_right, chunk, index, shifts, constants_right, fns_right);

        digest[0] += words_left[1] + words_right[2];
        digest[1] += words_left[2] + words_right[3];
        digest[2] += words_left[3] + words_right[4];
        digest[3] += words_left[4] + words_right[0];
        digest[4] += words_left[0] + words_right[1];

        std::rotate(digest, digest + 1, digest + 5);
    }

public:
    void hash(const UInt8 * data, size_t data_len) noexcept
    {
        UInt32 digest[5];
        for (size_t i = 0; i < 5; ++i)
            digest[i] = make_le32(initial_digest[i]);

        const UInt8 * last_chunk_start = data + (data_len & (~0x3f));
        while (data < last_chunk_start)
        {
            update(digest, reinterpret_cast<const UInt32 *>(data));
            data += 0x40;
        }

        UInt8 last_chunk[0x40] = {};
        UInt8 leftover_size = data_len & 0x3f;
        std::memcpy(last_chunk, data, leftover_size);

        last_chunk[leftover_size] = 0x80;

        if (leftover_size >= 0x38)
        {
            update(digest, reinterpret_cast<const UInt32 *>(last_chunk));
            std::memset(last_chunk, 0, 0x38);
        }

        UInt32 data_len_bits = static_cast<UInt32>(data_len << 3);
        std::memcpy(&last_chunk[0x38], &data_len_bits, sizeof(data_len_bits));
        data_len_bits = static_cast<UInt32>(data_len >> 29);
        std::memcpy(&last_chunk[0x3c], &data_len_bits, sizeof(data_len_bits));

        update(digest, reinterpret_cast<const UInt32 *>(last_chunk));

        for (size_t i = 0; i < 5; ++i)
        {
            UInt32 digest_part = make_le32(digest[i]);
            std::memcpy(digest_bytes + i * 4, &digest_part, 4);
        }
    }

    const UInt8 * get_digest_bytes() const noexcept { return digest_bytes; }
};


inline UInt256 ripeMD160Hash(const char * data, const size_t size) noexcept
{
    RIPEMD160 ripe;
    ripe.hash(reinterpret_cast<const UInt8 *>(data), size);

    UInt8 digest[20];
    std::memcpy(digest, ripe.get_digest_bytes(), sizeof(digest));

    std::reverse(digest, digest + sizeof(digest));

    UInt256 res = 0;
    std::memcpy(&res, digest, sizeof(digest));

    return res;
}
