#include <Server/ProxyProtocolHandler.h>

namespace DB
{

bool ProxyProtocolHandler::hasInitiatorPeerAddress() const
{
    return initiatorPeer.has_value();
}

const Poco::Net::IPAddress & ProxyProtocolHandler::initiatorPeerAddress() const
{
    return initiatorPeer.value();
}

}
