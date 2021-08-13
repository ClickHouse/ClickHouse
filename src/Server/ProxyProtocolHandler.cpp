#include <Server/ProxyProtocolHandler.h>

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

namespace DB
{

const std::vector<Poco::Net::IPAddress> & ProxyProtocolHandler::getAddressChain() const
{
    return address_chain;
}

namespace Util
{

std::vector<Poco::Net::IPAddress> splitAddresses(const std::string & addresses_str)
{
    std::vector<Poco::Net::IPAddress> addresses;
    std::vector<std::string> addresses_vstr;
    boost::split(addresses_vstr, addresses_str, boost::is_any_of(","));
    for (auto & address_str : addresses_vstr)
    {
        boost::trim(address_str);
        addresses.emplace_back(address_str);
    }
    return addresses;
}

std::string joinAddresses(const std::vector<Poco::Net::IPAddress> & addresses)
{
    std::string addresses_str;
    for (const auto & address : addresses)
    {
        if (!addresses_str.empty())
            addresses_str += ',';
        addresses_str += address.toString();
    }
    return addresses_str;
}

}

}
