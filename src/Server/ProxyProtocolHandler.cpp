#include <Server/ProxyProtocolHandler.h>
#include <Server/HTTP/HTTPServerRequest.h>

#include "Poco/Net/StreamSocket.h"

#include <boost/algorithm/string.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int IP_ADDRESS_NOT_ALLOWED;
}

bool ProxyProtocolHandler::hasInitiatorPeerAddress() const
{
    return initiatorPeer.has_value();
}

const Poco::Net::IPAddress & ProxyProtocolHandler::initiatorPeerAddress() const
{
    return initiatorPeer.value();
}

PROXYProtocolHandler::PROXYProtocolHandler(const PROXYConfig & config_)
    : config(config_)
{
}

void PROXYProtocolHandler::handle(const Poco::Net::StreamSocket & socket)
{
    switch (config.version)
    {
        case PROXYConfig::Version::v1:
        {
            prevStreamParsedPeer = consumePROXYv1Header(socket);
            break;
        }
        case PROXYConfig::Version::v2:
        {
            prevStreamParsedPeer = consumePROXYv2Header(socket);
            break;
        }
    }

    initiatorPeer = prevStreamParsedPeer;
}

void PROXYProtocolHandler::handle(const HTTPServerRequest & request)
{
    if (!request.has("X-Forwarded-For"))
    {
        initiatorPeer = prevStreamParsedPeer;
        return;
    }

    auto address_str = request.get("X-Forwarded-For");
    const auto comma_pos = address_str.find(',');
    address_str = address_str.substr(0, comma_pos);
    boost::algorithm::trim(address_str);

    if (address_str.empty())
    {
        initiatorPeer = prevStreamParsedPeer;
        return;
    }

    const Poco::Net::IPAddress address(address_str);

    if (prevStreamParsedPeer.has_value() && prevStreamParsedPeer != address)
        throw Exception("Inconsistent TCP stream header vs. X-Forwarded-For values", ErrorCodes::IP_ADDRESS_NOT_ALLOWED);

    initiatorPeer = address;
}

std::optional<Poco::Net::IPAddress> PROXYProtocolHandler::consumePROXYv1Header(const Poco::Net::StreamSocket & socket)
{
    (void)socket;
    return {};
}

std::optional<Poco::Net::IPAddress> PROXYProtocolHandler::consumePROXYv2Header(const Poco::Net::StreamSocket & socket)
{
    (void)socket;
    return {};
}

}
