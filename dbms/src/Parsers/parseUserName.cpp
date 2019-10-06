#include <Parsers/parseUserName.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Access/AllowedHosts.h>
#include <Functions/likePatternToRegexp.h>
#include <Common/StringUtils/StringUtils.h>
#include <Poco/Net/IPAddress.h>
#include <boost/algorithm/string.hpp>


namespace DB
{
namespace
{
    using IPAddress = Poco::Net::IPAddress;

    void extractAllowedHostsFromIPPattern(const String & pattern, IPAddress::Family address_family, AllowedHosts & result)
    {
        size_t slash = pattern.find('/');
        if (slash != String::npos)
        {
            /// IP subnet, e.g. "192.168.0.0/16" or "192.168.0.0/255.255.0.0".
            result.addIPSubnet(pattern);
            return;
        }

        bool has_wildcard = (pattern.find_first_of("%_") != String::npos);
        if (has_wildcard)
        {
            /// IP subnet specified with one of the wildcard characters, e.g. "192.168.%.%".
            String wildcard_replaced_with_zero_bits = pattern;
            String wildcard_replaced_with_one_bits = pattern;
            if (address_family == IPAddress::IPv6)
            {
                boost::algorithm::replace_all(wildcard_replaced_with_zero_bits, "_", "0");
                boost::algorithm::replace_all(wildcard_replaced_with_zero_bits, "%", "0000");
                boost::algorithm::replace_all(wildcard_replaced_with_one_bits, "_", "f");
                boost::algorithm::replace_all(wildcard_replaced_with_one_bits, "%", "ffff");
            }
            else if (address_family == IPAddress::IPv4)
            {
                boost::algorithm::replace_all(wildcard_replaced_with_zero_bits, "%", "0");
                boost::algorithm::replace_all(wildcard_replaced_with_one_bits, "%", "255");
            }

            IPAddress prefix{wildcard_replaced_with_zero_bits};
            IPAddress mask = ~(prefix ^ IPAddress{wildcard_replaced_with_one_bits});
            result.addIPSubnet(prefix, mask);
            return;
        }

        /// Exact IP address.
        result.addIPAddress(pattern);
        return;
    }


    void extractAllowedHostsFromHostPattern(const String & pattern, AllowedHosts & result)
    {
        bool has_wildcard = (pattern.find_first_of("%_") != String::npos);
        if (has_wildcard)
        {
            result.addHostRegexp(likePatternToRegexp(pattern));
            return;
        }

        result.addHostName(pattern);
        return;
    }


    AllowedHosts extractAllowedHostsFromPattern(const String & pattern)
    {
        AllowedHosts result;

        if (pattern.empty())
        {
            result.addIPSubnet(AllowedHosts::IPSubnet::ALL_ADDRESSES);
            return result;
        }

        /// If `host` starts with digits and a dot then it's an IP pattern, otherwise it's a hostname pattern.
        size_t first_not_digit = pattern.find_first_not_of("0123456789");
        if ((first_not_digit != String::npos) && (first_not_digit != 0) && (pattern[first_not_digit] == '.'))
        {
            extractAllowedHostsFromIPPattern(pattern, IPAddress::IPv4, result);
            return result;
        }

        size_t first_not_hex = pattern.find_first_not_of("0123456789ABCDEFabcdef");
        if (((first_not_hex == 4) && pattern[first_not_hex] == ':')
                || startsWith(pattern, "::"))
        {
            extractAllowedHostsFromIPPattern(pattern, IPAddress::IPv6, result);
            return result;
        }

        extractAllowedHostsFromHostPattern(pattern, result);
        return result;
    }


    bool parseNameImpl(IParser::Pos & pos, Expected & expected, String & result, AllowedHosts * allowed_hosts)
    {
        if (!parseIdentifierOrStringLiteral(pos, expected, result))
            return false;

        boost::algorithm::trim(result);

        if (allowed_hosts)
        {
            allowed_hosts->clear();
            allowed_hosts->addIPSubnet(AllowedHosts::IPSubnet::ALL_ADDRESSES);
        }

        if (ParserToken{TokenType::At}.ignore(pos, expected))
        {
            String host;
            if (!parseIdentifierOrStringLiteral(pos, expected, host))
                return false;

            boost::algorithm::trim(host);
            if (host != "%")
            {
                result += "@" + host;
                if (allowed_hosts)
                    *allowed_hosts = extractAllowedHostsFromPattern(host);
            }
        }

        return true;
    }
}


bool parseUserName(IParser::Pos & pos, Expected & expected, String & result)
{
    return parseNameImpl(pos, expected, result, nullptr);
}


bool parseUserName(IParser::Pos & pos, Expected & expected, String & result, AllowedHosts & allowed_hosts)
{
    return parseNameImpl(pos, expected, result, &allowed_hosts);
}


bool parseRoleName(IParser::Pos & pos, Expected & expected, String & result)
{
    return parseNameImpl(pos, expected, result, nullptr);
}
}
