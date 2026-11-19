#pragma once

#include <Server/DistributedQuery/StreamingExchangeProtocol.h>
#include <Poco/Net/StreamSocket.h>
#include <base/types.h>

#include <functional>
#include <memory>

namespace DB
{

/// Authenticates an incoming exchange connection from the auth token presented in
/// SourceHello (empty when the peer sent none). Throws to reject the connection
/// before it is registered. Empty (default, and the case when no authenticator is
/// configured) means authentication is not enforced and the token is ignored; a
/// caller can inject a closure that verifies the token.
using ExchangeConnectionAuthenticator = std::function<void(const String & auth_token)>;

/// Serves the connections to the streaming-exchange port that open with a stateless worker control packet.
class IExchangeControlConnectionHandler
{
public:
    virtual ~IExchangeControlConnectionHandler() = default;

    /// Takes over `socket`, whose first packet header `header` is already read. Runs on the exchange server's handshake
    /// pool, so it returns once the connection's hello is handled.
    virtual void handleControlConnection(
        Poco::Net::StreamSocket socket, const StreamingExchangeProtocol::PacketHeader & header, const ExchangeConnectionAuthenticator & authenticate)
        = 0;
};

using ExchangeControlConnectionHandlerPtr = std::shared_ptr<IExchangeControlConnectionHandler>;

}
