#include "IPv6ToBinary.h"
#include <Poco/Net/IPAddress.h>
#include <Poco/ByteOrder.h>

#include <Common/formatIPv6.h>

#include <cstring>
#include <bit>


namespace DB
{

/// Result array could be indexed with all possible uint8 values without extra check.
/// For values greater than 128 we will store same value as for 128 (all bits set).
constexpr size_t IPV6_MASKS_COUNT = 256;
using RawMaskArrayV6 = std::array<uint8_t, IPV6_BINARY_LENGTH>;

void IPv6ToRawBinary(const Poco::Net::IPAddress & address, char * res)
{
    if (Poco::Net::IPAddress::IPv6 == address.family())
    {
        memcpy(res, address.addr(), 16);
    }
    else if (Poco::Net::IPAddress::IPv4 == address.family())
    {
        /// Convert to IPv6-mapped address.
        memset(res, 0, 10);
        res[10] = '\xFF';
        res[11] = '\xFF';
        memcpy(&res[12], address.addr(), 4);
    }
    else
        memset(res, 0, 16);
}

std::array<char, 16> IPv6ToBinary(const Poco::Net::IPAddress & address)
{
    std::array<char, 16> res;
    IPv6ToRawBinary(address, res.data());
    return res;
}

template <typename RawMaskArrayT>
static constexpr RawMaskArrayT generateBitMask(size_t prefix)
{
    RawMaskArrayT arr{0};
    if (prefix >= arr.size() * 8)
        prefix = arr.size() * 8;
    size_t i = 0;
    for (; prefix >= 8; ++i, prefix -= 8)
        arr[i] = 0xff;
    if (prefix > 0)
        arr[i++] = ~(0xff >> prefix);
    while (i < arr.size())
        arr[i++] = 0x00;
    return arr;
}

template <typename RawMaskArrayT, size_t masksCount>
static constexpr std::array<RawMaskArrayT, masksCount> generateBitMasks()
{
    std::array<RawMaskArrayT, masksCount> arr{};
    for (size_t i = 0; i < masksCount; ++i)
        arr[i] = generateBitMask<RawMaskArrayT>(i);
    return arr;
}

const std::array<uint8_t, 16> & getCIDRMaskIPv6(UInt8 prefix_len)
{
    static constexpr auto IPV6_RAW_MASK_ARRAY = generateBitMasks<RawMaskArrayV6, IPV6_MASKS_COUNT>();
    return IPV6_RAW_MASK_ARRAY[prefix_len];
}

bool matchIPv4Subnet(UInt32 addr, UInt32 cidr_addr, UInt8 prefix)
{
    UInt32 mask = (prefix >= 32) ? 0xffffffffu : ~(0xffffffffu >> prefix);
    return (addr & mask) == (cidr_addr & mask);
}

#if defined(__SSE2__)
#include <emmintrin.h>

bool matchIPv6Subnet(const uint8_t * addr, const uint8_t * cidr_addr, UInt8 prefix)
{
    uint16_t mask = _mm_movemask_epi8(_mm_cmpeq_epi8(
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(addr)),
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(cidr_addr))));
    mask = ~mask;

    if (mask)
    {
        auto offset = std::countr_zero(mask);

        if (prefix / 8 != offset)
            return prefix / 8 < offset;

        auto cmpmask = ~(0xff >> (prefix % 8));
        return (addr[offset] & cmpmask) == (cidr_addr[offset] & cmpmask);
    }
    return true;
}

# else

bool matchIPv6Subnet(const uint8_t * addr, const uint8_t * cidr_addr, UInt8 prefix)
{
    if (prefix > IPV6_BINARY_LENGTH * 8U)
        prefix = IPV6_BINARY_LENGTH * 8U;

    size_t i = 0;
    for (; prefix >= 8; ++i, prefix -= 8)
    {
        if (addr[i] != cidr_addr[i])
            return false;
    }
    if (prefix == 0)
        return true;

    auto mask = ~(0xff >> prefix);
    return (addr[i] & mask) == (cidr_addr[i] & mask);
}

#endif  // __SSE2__

}
