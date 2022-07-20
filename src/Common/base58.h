#pragma once
#include <climits>
#include <cstring>

namespace DB
{

inline size_t encodeBase58(const char8_t * src, char8_t * dst)
{
    const char * base58_encoding_alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

    size_t idx = 0;
    for (; *src; ++src)
    {
        unsigned int carry = static_cast<unsigned char>(*src);
        for (size_t j = 0; j < idx; ++j)
        {
            carry += static_cast<unsigned int>(dst[j] << 8);
            dst[j] = static_cast<unsigned char>(carry % 58);
            carry /= 58;
        }
        while (carry > 0)
        {
            dst[idx++] = static_cast<unsigned char>(carry % 58);
            carry /= 58;
        }
    }

    size_t c_idx = idx >> 1;
    for (size_t i = 0; i < c_idx; ++i)
    {
        char s = base58_encoding_alphabet[static_cast<unsigned char>(dst[i])];
        dst[i] = base58_encoding_alphabet[static_cast<unsigned char>(dst[idx - (i + 1)])];
        dst[idx - (i + 1)] = s;
    }
    if ((idx & 1))
    {
        dst[c_idx] = base58_encoding_alphabet[static_cast<unsigned char>(dst[c_idx])];
    }
    dst[idx] = '\0';
    return idx + 1;
}

inline size_t decodeBase58(const char8_t * src, char8_t * dst)
{
    const char map_digits[128]
        = {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
           -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 0,  1,  2,  3,  4,  5,  6,  7,  8,  -1, -1, -1, -1, -1, -1,
           -1, 9,  10, 11, 12, 13, 14, 15, 16, -1, 17, 18, 19, 20, 21, -1, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, -1, -1, -1, -1, -1,
           -1, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, -1, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, -1, -1, -1, -1, -1};

    size_t idx = 0;

    for (; *src; ++src)
    {
        unsigned int carry = map_digits[*src];
        if (unlikely(carry == UINT_MAX))
        {
            return 0;
        }
        for (size_t j = 0; j < idx; ++j)
        {
            carry += static_cast<unsigned char>(dst[j]) * 58;
            dst[j] = static_cast<unsigned char>(carry & 0xff);
            carry >>= 8;
        }
        while (carry > 0)
        {
            dst[idx++] = static_cast<unsigned char>(carry & 0xff);
            carry >>= 8;
        }
    }

    size_t c_idx = idx >> 1;
    for (size_t i = 0; i < c_idx; ++i)
    {
        char s = dst[i];
        dst[i] = dst[idx - (i + 1)];
        dst[idx - (i + 1)] = s;
    }
    dst[idx] = '\0';
    return idx + 1;
}

}
