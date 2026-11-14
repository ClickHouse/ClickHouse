#include <base/StringViewHash.h>

#if defined(ENABLE_EXPERIMENTAL_LONG_CRC32_HASH) && ENABLE_EXPERIMENTAL_LONG_CRC32_HASH \
    && defined(__aarch64__) && defined(__ARM_FEATURE_CRC32) && defined(__ARM_FEATURE_CRYPTO) \
    && defined(__ARM_NEON) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__

namespace
{
template <size_t Segment> inline UInt32 crc32HashFoldBlocks(UInt32 crc, const char *& data, size_t & size)
{
    static_assert(Segment == 64 || Segment == 256);
    constexpr UInt64 k0 = Segment == 64 ? 2826614952 : 2375439644;
    constexpr UInt64 k1 = Segment == 64 ? 1771228834 : 3178201592;
    constexpr UInt64 k2 = Segment == 64 ? 1947135746 : 3702618788;
    while (size >= 4 * Segment + 8)
    {
        UInt32 a = crc;
        UInt32 b = 0;
        UInt32 c = 0;
        UInt32 d = 0;
        for (size_t offset = 0; offset < Segment; offset += 8)
        {
            a = __crc32cd(a, unalignedLoadLittleEndian<UInt64>(data + offset));
            b = __crc32cd(b, unalignedLoadLittleEndian<UInt64>(data + Segment + offset));
            c = __crc32cd(c, unalignedLoadLittleEndian<UInt64>(data + 2 * Segment + offset));
            d = __crc32cd(d, unalignedLoadLittleEndian<UInt64>(data + 3 * Segment + offset));
        }
        crc = __crc32cd(d, unalignedLoadLittleEndian<UInt64>(data + 4 * Segment));
        crc ^= __crc32cd(0, UInt64(vmull_p64(a, k0)));
        crc ^= __crc32cd(0, UInt64(vmull_p64(b, k1)));
        crc ^= __crc32cd(0, UInt64(vmull_p64(c, k2)));
        data += 4 * Segment + 8;
        size -= 4 * Segment + 8;
    }
    return crc;
}

}

NO_INLINE size_t crc32HashLong(const char * data, size_t length, UInt32 seed)
{
    chassert(length >= 1024);
    const char * last = data + length - 8;
    size_t size = ((length - 1) / 8) * 8 - 24;
    data += 24;
    UInt32 crc = seed;
    if (length >= 16384)
        crc = crc32HashFoldBlocks<256>(crc, data, size);
    while (size >= 64)
    {
        for (size_t offset = 0; offset < 64; offset += 8)
            crc = __crc32cd(crc, unalignedLoadLittleEndian<UInt64>(data + offset));
        data += 64;
        size -= 64;
    }
    while (size)
    {
        crc = __crc32cd(crc, unalignedLoadLittleEndian<UInt64>(data));
        data += 8;
        size -= 8;
    }
    crc = __crc32cd(crc, unalignedLoadLittleEndian<UInt64>(last));
    return (UInt64(crc) << 32) | crc;
}

#endif
