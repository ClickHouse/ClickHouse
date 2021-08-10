#include <Server/HTTPProxyProtocolHandler.h>
#include <Server/HTTP/HTTPServerRequest.h>

#include <boost/algorithm/string.hpp>

#include <string>
#include <vector>

namespace DB
{

HTTPProxyProtocolHandler::HTTPProxyProtocolHandler(const HTTPProxyConfig & config_)
    : config(config_)
{
}

void HTTPProxyProtocolHandler::handle(const HTTPServerRequest & request)
{
    addressChain.clear();

    std::vector<std::string> addresses;
    boost::split(addresses, request.get("X-Forwarded-For", ""), boost::is_any_of(","));

    for (auto & address : addresses)
    {
        boost::trim(address);
        if (!address.empty())
            addressChain.emplace_back(address);
    }
}

}
