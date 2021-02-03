#include "IPv6ToBinary.h"
#include <Poco/Net/IPAddress.h>
#include <Poco/ByteOrder.h>

#include <Common/formatIPv6.h>

#include <cstring>


namespace DB
{

/// Result array could be indexed with all possible uint8 values without extra check.
/// For values greater than 128 we will store same value as for 128 (all bits set).
constexpr size_t IPV6_MASKS_COUNT = 256;

using RawMaskArray = std::array<uint8_t, IPV6_BINARY_LENGTH>;

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

static constexpr RawMaskArray generateBitMask(size_t prefix)
{
    if (prefix >= 128)
        prefix = 128;
    RawMaskArray arr{0};
    size_t i = 0;
    for (; prefix >= 8; ++i, prefix -= 8)
        arr[i] = 0xff;
    if (prefix > 0)
        arr[i++] = ~(0xff >> prefix);
    while (i < 16)
        arr[i++] = 0x00;
    return arr;
}

static constexpr std::array<RawMaskArray, IPV6_MASKS_COUNT> generateBitMasks()
{
    std::array<RawMaskArray, IPV6_MASKS_COUNT> arr{};
    for (size_t i = 0; i < IPV6_MASKS_COUNT; ++i)
        arr[i] = generateBitMask(i);
    return arr;
}

const uint8_t * getCIDRMaskIPv6(UInt8 prefix_len)
{
    static constexpr std::array<RawMaskArray, IPV6_MASKS_COUNT> IPV6_RAW_MASK_ARRAY = generateBitMasks();
    return IPV6_RAW_MASK_ARRAY[prefix_len].data();
}

}
