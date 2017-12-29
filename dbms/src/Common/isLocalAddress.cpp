#include <Common/isLocalAddress.h>

#include <cstring>
#include <Core/Types.h>
#include <Poco/Util/Application.h>
#include <Poco/Net/NetworkInterface.h>
#include <Poco/Net/SocketAddress.h>


namespace DB
{

bool isLocalAddress(const Poco::Net::SocketAddress & address, UInt16 clickhouse_port)
{
    static auto interfaces = Poco::Net::NetworkInterface::list();

    if (clickhouse_port == address.port())
    {
        return interfaces.end() != std::find_if(interfaces.begin(), interfaces.end(),
            [&] (const Poco::Net::NetworkInterface & interface)
            {
                /** Compare the addresses without taking into account `scope`.
                  * Theoretically, this may not be correct - depends on `route` setting
                  *  - through which interface we will actually access the specified address.
                  */
                return interface.address().length() == address.host().length()
                    && 0 == memcmp(interface.address().addr(), address.host().addr(), address.host().length());
            });
    }

    return false;
}

}
