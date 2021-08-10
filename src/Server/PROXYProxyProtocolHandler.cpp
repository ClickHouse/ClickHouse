#include <Server/PROXYProxyProtocolHandler.h>
#include <Common/Exception.h>

#include "Poco/Net/StreamSocket.h"

#include <boost/algorithm/string.hpp>

#include <algorithm>
#include <string>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int IP_ADDRESS_NOT_ALLOWED;
    extern const int PROXY_PROTOCOL_HEADER_ERROR;
}

namespace
{

struct PROXYHeader
{
    Poco::Net::IPAddress initiator;
};

std::optional<PROXYHeader> tryReadPROXYHeader(Poco::Net::StreamSocket & socket, bool expect_v1, bool expect_v2)
{
    (void)socket;
    (void)expect_v1;
    (void)expect_v2;

    return {};
}

}

PROXYProxyProtocolHandler::PROXYProxyProtocolHandler(const PROXYProxyConfig & config_)
    : config(config_)
{
}

void PROXYProxyProtocolHandler::handle(Poco::Net::StreamSocket & socket)
{
    addressChain.clear();

    const auto expect_v1 = (config.version != PROXYProxyConfig::Version::v2);
    const auto expect_v2 = (config.version != PROXYProxyConfig::Version::v1);
    const auto header = tryReadPROXYHeader(socket, expect_v1, expect_v2);

    if (header.has_value())
        addressChain.emplace_back(header.value().initiator);
}

}
