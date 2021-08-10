#pragma once

#include "Poco/Net/IPAddress.h"

#include <optional>

namespace Poco { namespace Net { class StreamSocket; } }

namespace DB
{

class HTTPServerRequest;

class ProxyProtocolHandler
{
public:
    virtual ~ProxyProtocolHandler() = default;

    bool hasInitiatorPeerAddress() const;

    const Poco::Net::IPAddress & initiatorPeerAddress() const;

protected:
    std::optional<Poco::Net::IPAddress> initiatorPeer;
};

class TCPProxyProtocolHandler : virtual public ProxyProtocolHandler
{
public:
    virtual void handle(const Poco::Net::StreamSocket & socket) = 0;
};

class HTTPProxyProtocolHandler : virtual public ProxyProtocolHandler
{
public:
    virtual void handle(const HTTPServerRequest & request) = 0;
};

}
