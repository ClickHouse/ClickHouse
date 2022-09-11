#include <Common/base58.h>


namespace DB
{

size_t encodeBase58(const UInt8 * src, size_t src_length, UInt8 * dst)
{
    const char * base58_encoding_alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

    size_t processed = 0;
    size_t zeros = 0;

    while (*src == '\0' && processed < src_length - 1)
    {
        ++processed;
        ++zeros;
        *dst = '1';
        ++dst;
        ++src;
    }

    size_t idx = 0;

    while (processed < src_length - 1)
    {
        UInt8 carry = static_cast<UInt8>(*src);

        for (size_t j = 0; j < idx; ++j)
        {
            carry += static_cast<UInt32>(dst[j] << 8);
            dst[j] = static_cast<UInt8>(carry % 58);
            carry /= 58;
        }

        while (carry > 0)
        {
            dst[idx] = static_cast<UInt8>(carry % 58);
            ++idx;
            carry /= 58;
        }
        ++src;
        ++processed;
    }

    size_t c_idx = idx >> 1;
    for (size_t i = 0; i < c_idx; ++i)
    {
        char s = base58_encoding_alphabet[static_cast<UInt8>(dst[i])];
        dst[i] = base58_encoding_alphabet[static_cast<UInt8>(dst[idx - (i + 1)])];
        dst[idx - (i + 1)] = s;
    }

    if ((idx & 1))
    {
        dst[c_idx] = base58_encoding_alphabet[static_cast<UInt8>(dst[c_idx])];
    }

    dst[idx] = '\0';
    return zeros + idx + 1;
}


size_t decodeBase58(const UInt8 * src, size_t srclen, UInt8 * dst)
{
    static const Int8 map_digits[128] =
    {
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1,  0,  1,  2,  3,  4,  5,  6,  7,  8, -1, -1, -1, -1, -1, -1,
        -1,  9, 10, 11, 12, 13, 14, 15, 16, -1, 17, 18, 19, 20, 21, -1,
        22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, -1, -1, -1, -1, -1,
        -1, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, -1, 44, 45, 46,
        47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, -1, -1, -1, -1, -1
    };

    size_t processed = 0;
    size_t zeros = 0;

    while (*src == '1' && processed < srclen - 1)
    {
        ++processed;
        ++zeros;
        *dst = '\0';
        ++dst;
        ++src;
    }

    size_t idx = 0;

    while (processed < srclen-1)
    {
        UInt32 carry = map_digits[*src];
        if (carry == static_cast<UInt32>(-1))
        {
            return 0;
        }
        for (size_t j = 0; j < idx; ++j)
        {
            carry += static_cast<UInt8>(dst[j]) * 58;
            dst[j] = static_cast<UInt8>(carry & 0xFF);
            carry >>= 8;
        }
        while (carry > 0)
        {
            dst[idx] = static_cast<UInt8>(carry & 0xFF);
            ++idx;
            carry >>= 8;
        }
        ++src;
        ++processed;
    }

    size_t c_idx = idx >> 1;
    for (size_t i = 0; i < c_idx; ++i)
    {
        char s = dst[i];
        dst[i] = dst[idx - (i + 1)];
        dst[idx - (i + 1)] = s;
    }

    dst[idx] = '\0';
    return zeros + idx + 1;
}

}
