#include <Common/Base32.h>


namespace DB
{

namespace
{
    Int8 getInitValue(UInt8 elem)
    {
        if (elem >= 'A' && elem <= 'Z')
            return elem - 'A';
        else if (elem >= '2' && elem <= '7')
            return elem - '2' + 26;
        return -1;
    }
}

void encodeBase32(const UInt8 * src, size_t src_length, UInt8 * dst)
{
    const char * base32_encoding_alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

    const size_t read_once = 5;
    const Int8 swifts[6] = {0, 2, 4, 1, 3, 0};

    size_t dst_i = 0;

    for (size_t i = 0; i < src_length; i += read_once)
    {
        size_t read_count = std::min(read_once, src_length - i);
        UInt64 cur_data = 0;

        for (size_t j = i; j < i + read_count; ++j)
        {
            cur_data <<= 8;
            cur_data |= src[j];
        }

        cur_data <<= swifts[read_count];

        size_t write_count = (read_count * 8 + 4) / 5;
        Int8 shift = (write_count - 1) * 5;
        UInt64 bit_mask = (0x01Fll << shift);
        for (size_t j = 0; j < write_count; ++j)
        {
            dst[dst_i + j] = base32_encoding_alphabet[(cur_data & bit_mask) >> shift];
            shift -= 5;
            bit_mask >>= 5;
        }

        for (size_t j = 0; j < 8 - write_count; ++j)
        {
            dst[dst_i + j + write_count] = '=';
        }

        dst_i += 8;
    }
}


std::optional<size_t> decodeBase32(const UInt8 * src, size_t src_length, UInt8 * dst)
{
    size_t result = 0;

    const size_t read_once = 8;
    const Int8 write_count[8] = {-1, -1, 1, -1, 2, 3, -1, 4};
    const Int8 swifts[6] = {0, 2, 4, 1, 3, 0};

    size_t dst_i = 0;

    for (size_t i = 0; i < src_length; i += read_once)
    {
        Int8 cur_write_count = 5;
        UInt64 cur_data = 0;

        for (size_t j = 0; j < read_once; ++j)
        {
            if (src[i + j] == '=')
            {
                cur_write_count = write_count[j];
                break;
            }
            cur_data <<= 5;
            auto val = getInitValue(src[i + j]);
            if (val == -1)
                return std::nullopt;
            cur_data |= val;
        }

        if (cur_write_count == -1)
            return std::nullopt;

        result += cur_write_count;
        cur_data >>= swifts[cur_write_count];

        Int8 shift = (cur_write_count - 1) * 8;
        UInt64 bit_mask = (0xFFll << shift);
        for (Int8 j = 0; j < cur_write_count; ++j)
        {
            dst[dst_i + j] = (cur_data & bit_mask) >> shift;
            shift -= 8;
            bit_mask >>= 8;
        }

        dst_i += 5;
    }
    return result;
}

}
