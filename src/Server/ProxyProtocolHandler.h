#pragma once

#include "Poco/Net/IPAddress.h"

#include <vector>

namespace Poco { namespace Net { class StreamSocket; } }

namespace DB
{

class HTTPServerRequest;

class ProxyProtocolHandler
{
public:
    virtual ~ProxyProtocolHandler() = default;

    const std::vector<Poco::Net::IPAddress> & peerAddressChain() const;

protected:
    std::vector<Poco::Net::IPAddress> addressChain;
};

class StreamSocketAwareProxyProtocolHandler : virtual public ProxyProtocolHandler
{
public:
    virtual void handle(Poco::Net::StreamSocket & socket) = 0;
};

class HTTPServerRequestAwareProxyProtocolHandler : virtual public ProxyProtocolHandler
{
public:
    virtual void handle(const HTTPServerRequest & request) = 0;
};

}
