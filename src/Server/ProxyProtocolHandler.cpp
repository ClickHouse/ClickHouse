#include <Server/ProxyProtocolHandler.h>

namespace DB
{

const std::vector<Poco::Net::IPAddress> & ProxyProtocolHandler::peerAddressChain() const
{
    return addressChain;
}

}
