#include <Common/isLocalAddress.h>

#include <cstring>
#include <common/types.h>
#include <Poco/Util/Application.h>
#include <Poco/Net/NetworkInterface.h>
#include <Poco/Net/SocketAddress.h>


namespace DB
{

bool isLocalAddress(const Poco::Net::IPAddress & address)
{
    static auto interfaces = Poco::Net::NetworkInterface::list();

    return interfaces.end() != std::find_if(interfaces.begin(), interfaces.end(),
                [&] (const Poco::Net::NetworkInterface & interface)
                {
                    /** Compare the addresses without taking into account `scope`.
                      * Theoretically, this may not be correct - depends on `route` setting
                      *  - through which interface we will actually access the specified address.
                      */
                    return interface.address().length() == address.length()
                        && 0 == memcmp(interface.address().addr(), address.addr(), address.length());
                });
}

bool isLocalAddress(const Poco::Net::SocketAddress & address, UInt16 clickhouse_port)
{
    return clickhouse_port == address.port() && isLocalAddress(address.host());
}


size_t getHostNameDifference(const std::string & local_hostname, const std::string & host)
{
    size_t hostname_difference = 0;
    for (size_t i = 0; i < std::min(local_hostname.length(), host.length()); ++i)
        if (local_hostname[i] != host[i])
            ++hostname_difference;
    return hostname_difference;
}

}
