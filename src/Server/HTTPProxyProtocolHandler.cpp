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
    address_chain.clear();

    std::vector<std::string> addresses_str;
    boost::split(addresses_str, request.get("X-Forwarded-For", ""), boost::is_any_of(","));

    for (std::size_t i = 0; i < addresses_str.size(); ++i)
    {
        auto & address_str = addresses_str[i];
        boost::trim(address_str);

        const auto client_address = (i == 0);
        if (client_address)
        {
            if (address_str.empty())
            {
                address_chain.clear();
                return;
            }
        }
        else
        {
            if (address_str.empty())
                continue;

            if (!Util::addrInNet(boost::asio::ip::make_address(address_str), config.trusted_networks))
            {
                address_chain.clear();
                return;
            }
        }

        address_chain.emplace_back(address_str);
    }
}

}
