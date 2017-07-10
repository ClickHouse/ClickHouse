#include <IO/WriteHelpers.h>
#include <inttypes.h>
#include <stdio.h>

namespace DB
{

void formatHex(const UInt8 * __restrict src, UInt8 * __restrict dst, const size_t num_bytes)
{
    /// More optimal than lookup table by nibbles.
    constexpr auto hex =
        "000102030405060708090a0b0c0d0e0f"
        "101112131415161718191a1b1c1d1e1f"
        "202122232425262728292a2b2c2d2e2f"
        "303132333435363738393a3b3c3d3e3f"
        "404142434445464748494a4b4c4d4e4f"
        "505152535455565758595a5b5c5d5e5f"
        "606162636465666768696a6b6c6d6e6f"
        "707172737475767778797a7b7c7d7e7f"
        "808182838485868788898a8b8c8d8e8f"
        "909192939495969798999a9b9c9d9e9f"
        "a0a1a2a3a4a5a6a7a8a9aaabacadaeaf"
        "b0b1b2b3b4b5b6b7b8b9babbbcbdbebf"
        "c0c1c2c3c4c5c6c7c8c9cacbcccdcecf"
        "d0d1d2d3d4d5d6d7d8d9dadbdcdddedf"
        "e0e1e2e3e4e5e6e7e8e9eaebecedeeef"
        "f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff";

    size_t src_pos = 0;
    size_t dst_pos = 0;
    for (; src_pos < num_bytes; ++src_pos)
    {
        memcpy(&dst[dst_pos], &hex[src[src_pos] * 2], 2);
        dst_pos += 2;
    }
}

void formatUUID(const UInt8 * src16, UInt8 * dst36)
{
    formatHex(&src16[0], &dst36[0], 4);
    dst36[8] = '-';
    formatHex(&src16[4], &dst36[9], 2);
    dst36[13] = '-';
    formatHex(&src16[6], &dst36[14], 2);
    dst36[18] = '-';
    formatHex(&src16[8], &dst36[19], 2);
    dst36[23] = '-';
    formatHex(&src16[10], &dst36[24], 6);
}

void formatUUID(const UInt128 & uuid, UInt8 * dst36)
{
    char s[16+1];

    dst36[8] = '-';
    dst36[13] = '-';
    dst36[18] = '-';
    dst36[23] = '-';

    snprintf(s, sizeof(s), "%016llx", static_cast<long long>(uuid.low));

    memcpy(&dst36[0], &s, 8);
    memcpy(&dst36[9], &s[8], 4);
    memcpy(&dst36[14], &s[12], 4);

    snprintf(s, sizeof(s), "%016llx", static_cast<long long>(uuid.high));

    memcpy(&dst36[19], &s[0], 4);
    memcpy(&dst36[24], &s[4], 12);

}



void writeException(const Exception & e, WriteBuffer & buf)
{
    writeBinary(e.code(), buf);
    writeBinary(String(e.name()), buf);
    writeBinary(e.displayText(), buf);
    writeBinary(e.getStackTrace().toString(), buf);

    bool has_nested = e.nested() != nullptr;
    writeBinary(has_nested, buf);

    if (has_nested)
        writeException(Exception(*e.nested()), buf);
}

}
