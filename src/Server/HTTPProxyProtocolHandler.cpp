#include <Server/HTTPProxyProtocolHandler.h>
#include <Server/HTTP/HTTPServerRequest.h>

#include <boost/algorithm/string.hpp>

#include <string>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNTRUSTED_PROXY;
}

HTTPProxyProtocolHandler::HTTPProxyProtocolHandler(const HTTPProxyConfig & config_)
    : config(config_)
{
}

void HTTPProxyProtocolHandler::handle(const HTTPServerRequest & request)
{
    address_chain.clear();

    std::vector<Poco::Net::IPAddress> addresses;
    std::vector<std::string> addresses_str;
    boost::split(addresses_str, request.get("X-Forwarded-For", ""), boost::is_any_of(","));

    for (auto & address_str : addresses_str)
    {
        boost::trim(address_str);
        addresses.emplace_back(address_str);
    }

    std::size_t trim_front_n = 0;
    if (config.proxy_chain_limit > 0 && config.proxy_chain_limit < addresses.size())
        trim_front_n = addresses.size() - config.proxy_chain_limit;

    for (std::size_t i = trim_front_n; i < addresses.size(); ++i)
    {
        // The first address is the client address, we don't need to verify it against trusted_networks.
        if (i == trim_front_n || Util::addrInNet(addresses[i], config.trusted_networks))
        {
            address_chain.push_back(std::move(addresses[i]));
        }
        else
        {
            address_chain.clear();
            throw Exception("Untrusted proxy in the relevant chain of forwarded addresses", ErrorCodes::UNTRUSTED_PROXY);
        }
    }
}

}
