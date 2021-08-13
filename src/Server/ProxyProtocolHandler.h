#pragma once

#include "Poco/Net/IPAddress.h"

#include <vector>

namespace Poco::Net
{

class StreamSocket;
class IPAddress;

}

namespace DB
{

class HTTPServerRequest;

class ProxyProtocolHandler
{
public:
    virtual ~ProxyProtocolHandler() = default;

    const std::vector<Poco::Net::IPAddress> & getAddressChain() const;

protected:
    std::vector<Poco::Net::IPAddress> address_chain;
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

namespace Util
{

std::vector<Poco::Net::IPAddress> splitAddresses(const std::string & addresses_str);
std::string joinAddresses(const std::vector<Poco::Net::IPAddress> & addresses);

}

}
