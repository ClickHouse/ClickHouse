#include "IPv6ToBinary.h"
#include <Poco/Net/IPAddress.h>
#include <Poco/ByteOrder.h>

#include <cstring>


namespace DB
{

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

}
