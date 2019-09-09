#include "IPv6ToBinary.h"
#include <Poco/Net/IPAddress.h>
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

}
