#pragma once

#include <Server/ProxyConfig.h>

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

class PROXYProtocolHandler final
    : public TCPProxyProtocolHandler
    , public HTTPProxyProtocolHandler
{
public:
    explicit PROXYProtocolHandler(const PROXYConfig & config_);

    virtual void handle(const Poco::Net::StreamSocket & socket) override;
    virtual void handle(const HTTPServerRequest & request) override;

private:
    static std::optional<Poco::Net::IPAddress> consumePROXYv1Header(const Poco::Net::StreamSocket & socket);
    static std::optional<Poco::Net::IPAddress> consumePROXYv2Header(const Poco::Net::StreamSocket & socket);

private:
    PROXYConfig config;
    std::optional<Poco::Net::IPAddress> prevStreamParsedPeer;
};

}
