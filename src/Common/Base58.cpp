#include <Common/Base58.h>
#include <Common/Base58fd.h>
#include <Common/TargetSpecific.h>

namespace DB
{

size_t encodeBase58(const UInt8 * src, size_t src_length, UInt8 * dst)
{
    const char * base58_encoding_alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

    size_t processed = 0;
    size_t idx = 0;
    size_t zeros = 0;

    while (processed < src_length && *src == 0)
    {
        ++processed;
        ++zeros;
        *dst = '1';
        ++dst;
        ++src;
    }

    while (processed < src_length)
    {
        UInt32 carry = *src;

        for (size_t j = 0; j < idx; ++j)
        {
            carry += static_cast<UInt32>(dst[j]) << 8;
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
        char s = base58_encoding_alphabet[dst[i]];
        dst[i] = base58_encoding_alphabet[dst[idx - (i + 1)]];
        dst[idx - (i + 1)] = s;
    }

    if ((idx & 1))
    {
        dst[c_idx] = base58_encoding_alphabet[dst[c_idx]];
    }

    return zeros + idx;
}


std::optional<size_t> decodeBase58(const UInt8 * src, size_t src_length, UInt8 * dst)
{
    // clang-format off
    static const Int8 map_digits[256] =
    {
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1,  0,  1,  2,  3,  4,  5,  6,  7,  8, -1, -1, -1, -1, -1, -1,
        -1,  9, 10, 11, 12, 13, 14, 15, 16, -1, 17, 18, 19, 20, 21, -1,
        22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, -1, -1, -1, -1, -1,
        -1, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, -1, 44, 45, 46,
        47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    };
    // clang-format on

    size_t processed = 0;
    size_t idx = 0;
    size_t zeros = 0;

    while (processed < src_length && *src == '1')
    {
        ++processed;
        ++zeros;
        *dst = '\0';
        ++dst;
        ++src;
    }

    while (processed < src_length)
    {
        Int8 digit = map_digits[*src];
        UInt32 carry = digit == -1 ? 0xFFFFFFFFU : static_cast<UInt32>(digit);
        if (carry == 0xFFFFFFFFU)
        {
            return {};
        }
        for (size_t j = 0; j < idx; ++j)
        {
            carry += dst[j] * 58;
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
        UInt8 s = dst[i];
        dst[i] = dst[idx - (i + 1)];
        dst[idx - (i + 1)] = s;
    }

    return zeros + idx;
}

size_t encodeBase58_32(const UInt8 * src, UInt8 * dst)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v3))
        return TargetSpecific::x86_64_v3::encodeBase58_32(reinterpret_cast<const uint8_t *>(src), reinterpret_cast<uint8_t *>(dst));
    else
#endif
        return TargetSpecific::Default::encodeBase58_32(reinterpret_cast<const uint8_t *>(src), reinterpret_cast<uint8_t *>(dst));
}

size_t encodeBase58_64(const UInt8 * src, UInt8 * dst)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v3))
        return TargetSpecific::x86_64_v3::encodeBase58_64(reinterpret_cast<const uint8_t *>(src), reinterpret_cast<uint8_t *>(dst));
    else
#endif
        return TargetSpecific::Default::encodeBase58_64(reinterpret_cast<const uint8_t *>(src), reinterpret_cast<uint8_t *>(dst));
}

std::optional<size_t> decodeBase58_32(const UInt8 * src, size_t src_length, UInt8 * dst)
{
    return TargetSpecific::Default::decodeBase58_32(reinterpret_cast<const uint8_t *>(src), src_length, reinterpret_cast<uint8_t *>(dst));
}

std::optional<size_t> decodeBase58_64(const UInt8 * src, size_t src_length, UInt8 * dst)
{
    return TargetSpecific::Default::decodeBase58_64(reinterpret_cast<const uint8_t *>(src), src_length, reinterpret_cast<uint8_t *>(dst));
}

}
