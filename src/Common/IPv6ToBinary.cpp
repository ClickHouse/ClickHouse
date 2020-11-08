#include "IPv6ToBinary.h"
#include <Poco/Net/IPAddress.h>
#include <Poco/ByteOrder.h>

#include <cstring>


namespace DB
{

std::array<char, 16> IPv6ToBinary(const Poco::Net::IPAddress & address)
{
    std::array<char, 16> res;

    if (Poco::Net::IPAddress::IPv6 == address.family())
    {
        memcpy(res.data(), address.addr(), 16);
    }
    else if (Poco::Net::IPAddress::IPv4 == address.family())
    {
        /// Convert to IPv6-mapped address.
        memset(res.data(), 0, 10);
        res[10] = '\xFF';
        res[11] = '\xFF';
        memcpy(&res[12], address.addr(), 4);
    }
    else
        memset(res.data(), 0, 16);

    return res;
}


UInt32 IPv4ToBinary(const Poco::Net::IPAddress & address, bool & success)
{
    if (!address.isIPv4Mapped())
    {
        success = false;
        return 0;
    }

    success = true;
    if (Poco::Net::IPAddress::IPv6 == address.family())
    {
        auto raw = reinterpret_cast<const uint8_t *>(address.addr());
        return *reinterpret_cast<const UInt32 *>(&raw[12]);
    }
    else if (Poco::Net::IPAddress::IPv4 == address.family())
    {
        auto raw = reinterpret_cast<const uint8_t *>(address.addr());
        return *reinterpret_cast<const UInt32 *>(raw);
    }

    success = false;
    return 0;
}

}
