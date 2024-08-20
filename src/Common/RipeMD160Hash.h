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

/// https://homes.esat.kuleuven.be/~bosselae/ripemd160/pdf/AB-9601/AB-9601.pdf
/// https://en.wikipedia.org/wiki/RIPEMD

class RipeMD160
{
private:
    using FuncPtr = UInt32 (*)(UInt32, UInt32, UInt32);

    /// Stores the final 20-byte (160-bit) hash result
    UInt8 digest_bytes[20];

    static constexpr UInt32 initial_hash_values[5] = {0x67452301UL, 0xEFCDAB89UL, 0x98BADCFEUL, 0x10325476UL, 0xC3D2E1F0UL};

    static constexpr UInt8 rho_order[16] = {0x7, 0x4, 0xD, 0x1, 0xA, 0x6, 0xF, 0x3, 0xC, 0x0, 0x9, 0x5, 0x2, 0xE, 0xB, 0x8};

    static constexpr UInt8 shift_amounts[80]
        = {11, 14, 15, 12, 5,  8,  7,  9,  11, 13, 14, 15, 6,  7,  9,  8,  12, 13, 11, 15, 6,  9,  9,  7,  12, 15, 11,
           13, 7,  8,  7,  7,  13, 15, 14, 11, 7,  7,  6,  8,  13, 14, 13, 12, 5,  5,  6,  9,  14, 11, 12, 14, 8,  6,
           5,  5,  15, 12, 15, 14, 9,  9,  8,  6,  15, 12, 13, 13, 9,  5,  8,  6,  14, 11, 12, 11, 8,  6,  5,  5};

     static constexpr UInt32 left_round_constants[5] = {0x00000000UL, 0x5A827999UL, 0x6ED9EBA1UL, 0x8F1BBCDCUL, 0xA953FD4EUL};

    static constexpr UInt32 right_round_constants[5] = {0x50A28BE6UL, 0x5C4DD124UL, 0x6D703EF3UL, 0x7A6D76E9UL, 0x00000000UL};

    static constexpr UInt8 left_function_order[5] = {1, 2, 3, 4, 5};

    static constexpr UInt8 right_function_order[5] = {5, 4, 3, 2, 1};

    static ALWAYS_INLINE UInt32 F_1(UInt32 a, UInt32 b, UInt32 c) noexcept { return (a ^ b ^ c); }

    static ALWAYS_INLINE UInt32 F_2(UInt32 a, UInt32 b, UInt32 c) noexcept { return ((a & b) | (~a & c)); }

    static ALWAYS_INLINE UInt32 F_3(UInt32 a, UInt32 b, UInt32 c) noexcept { return ((a | ~b) ^ c); }

    static ALWAYS_INLINE UInt32 F_4(UInt32 a, UInt32 b, UInt32 c) noexcept { return ((a & c) | (b & ~c)); }

    static ALWAYS_INLINE UInt32 F_5(UInt32 a, UInt32 b, UInt32 c) noexcept { return (a ^ (b | ~c)); }

    static constexpr FuncPtr hash_functions[5] = {F_1, F_2, F_3, F_4, F_5};

    static ALWAYS_INLINE FuncPtr get_function(UInt8 function_id) noexcept { return hash_functions[function_id - 1]; }

    static ALWAYS_INLINE UInt32 convert_to_little_endian(UInt32 x) noexcept
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

    static ALWAYS_INLINE UInt32 rotate_left(UInt32 value, UInt32 shift) noexcept { return (value << shift) | (value >> (32 - shift)); }

    /// Performs one full pass (5 rounds) of RIPEMD-160 algorithm for one path (left or right)
    void process_rounds(
        UInt32 * current_digest,
        UInt32 * temp_words,
        const UInt32 * data_chunk,
        UInt8 * index_order,
        const UInt8 * shift_values,
        const UInt32 * round_constants,
        const UInt8 * function_order) noexcept
    {
        std::memcpy(temp_words, current_digest, 5 * sizeof(UInt32));
        for (UInt8 round = 0; round < 5; ++round)
        {
            UInt32 k = round_constants[round];
            UInt8 fn = function_order[round];
            for (UInt8 j = 0; j < 16; ++j)
            {
                UInt32 temp_result = get_function(fn)(temp_words[1], temp_words[2], temp_words[3]);
                temp_result += temp_words[0] + convert_to_little_endian(data_chunk[index_order[j]]) + k;
                temp_result = rotate_left(temp_result, shift_values[index_order[j]]) + temp_words[4];
                temp_words[0] = temp_words[4];
                temp_words[4] = temp_words[3];
                temp_words[3] = rotate_left(temp_words[2], 10);
                temp_words[2] = temp_words[1];
                temp_words[1] = temp_result;
            }
            shift_values += 16;
            UInt8 reordered_index[16];
            for (size_t i = 0; i < 16; ++i)
                reordered_index[i] = rho_order[index_order[i]];
            std::memcpy(index_order, reordered_index, 16);
        }
    }

    /// Update the digest with the given chunk of data
    void update_digest(UInt32 * current_digest, const UInt32 * data_chunk) noexcept
    {
        UInt8 index_order[16];
        for (UInt8 i = 0; i < 16; ++i)
            index_order[i] = i;

        UInt32 left_path_words[5];
        process_rounds(current_digest, left_path_words, data_chunk, index_order, shift_amounts, left_round_constants, left_function_order);

        static constexpr UInt8 rho_reordered_index[16] = {5, 14, 7, 0, 9, 2, 11, 4, 13, 6, 15, 8, 1, 10, 3, 12};
        std::memcpy(index_order, rho_reordered_index, 16);

        UInt32 right_path_words[5];
        process_rounds(
            current_digest, right_path_words, data_chunk, index_order, shift_amounts, right_round_constants, right_function_order);

        current_digest[0] += left_path_words[1] + right_path_words[2];
        current_digest[1] += left_path_words[2] + right_path_words[3];
        current_digest[2] += left_path_words[3] + right_path_words[4];
        current_digest[3] += left_path_words[4] + right_path_words[0];
        current_digest[4] += left_path_words[0] + right_path_words[1];

        std::rotate(current_digest, current_digest + 1, current_digest + 5);
    }

public:
    void hash(const UInt8 * data, size_t data_len) noexcept
    {
        UInt32 digest[5];
        for (size_t i = 0; i < 5; ++i)
            digest[i] = convert_to_little_endian(initial_hash_values[i]);

        const UInt8 * last_chunk_start = data + (data_len & (~0x3F));
        while (data < last_chunk_start)
        {
            update_digest(digest, reinterpret_cast<const UInt32 *>(data));
            data += 0x40;
        }

        UInt8 last_chunk[0x40] = {};
        UInt8 leftover_size = data_len & 0x3F;
        std::memcpy(last_chunk, data, leftover_size);

        last_chunk[leftover_size] = 0x80;

        if (leftover_size >= 0x38)
        {
            update_digest(digest, reinterpret_cast<const UInt32 *>(last_chunk));
            std::memset(last_chunk, 0, 0x38);
        }

        UInt32 data_len_bits = static_cast<UInt32>(data_len << 3);
        std::memcpy(&last_chunk[0x38], &data_len_bits, sizeof(data_len_bits));
        data_len_bits = static_cast<UInt32>(data_len >> 29);
        std::memcpy(&last_chunk[0x3C], &data_len_bits, sizeof(data_len_bits));

        update_digest(digest, reinterpret_cast<const UInt32 *>(last_chunk));

        for (size_t i = 0; i < 5; ++i)
        {
            UInt32 digest_part = convert_to_little_endian(digest[i]);
            std::memcpy(digest_bytes + i * 4, &digest_part, 4);
        }
    }

    const UInt8 * get_digest_bytes() const noexcept { return digest_bytes; }
};


inline UInt256 ripeMD160Hash(const char * data, const size_t size) noexcept
{
    RipeMD160 ripe;
    ripe.hash(reinterpret_cast<const UInt8 *>(data), size);

    UInt8 digest[20];
    std::memcpy(digest, ripe.get_digest_bytes(), sizeof(digest));

    std::reverse(digest, digest + sizeof(digest));

    UInt256 res = 0;
    std::memcpy(&res, digest, sizeof(digest));

    return res;
}
