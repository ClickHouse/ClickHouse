#include <Parsers/parseUserName.h>
#include <Parsers/parseIdentifierOrStringLiteral.h>


namespace DB
{
namespace
{
    template <bool ipV6>
    AllowedHosts extractAllowedHostsFromIP(const String & pattern)
    {
        std::string_view view = pattern;
        if constexpr (ipV6)
        {
            if (!view.empty() && (view[0] == "[") && (view[view.length() - 1] == "]"))
            {
                view.remove_prefix(1);
                view.remove_suffix(1);
            }
        }

        auto family = ipV6 ? Poco::Net::IPAddress::Family::IPv6 : Poco::Net::IPAddress::Family::IPv4;

        AllowedHosts result;
        size_t first_wildcard = view.find_first_of("%_");
        if (first_wildcard == String::npos)
        {
            /// No wildcard characters, it's an exact IP address or IP subnet.
            size_t slash = pattern.find('/');
            if (slash == String::npos)
            {
                /// Exact IP address.
                result.addIPAddress(Poco::Net::IPAddress{view});
                return result;
            }

            /// IP subnet.
            auto prefix = view.substr(0, slash);
            auto mask = view.substr(slash + 1, view.length() - slash - 1);
            if (std::all_of(mask.begin(), mask.end(), isNumericASCII))
                result.addIPSubnet(Poco::Net::IPAddress(prefix, family), parseFromString<UInt8>(mask));
            else
                result.addIPSubnet(Poco::Net::IPAddress(prefix, family), Poco::Net::IPAddress(mask, family));
            return result;
        }

        /// Calculate number of prefix components.
        size_t num_prefix_components = 0;
        for (size_t i = 0; i != first_wildcard; ++i)
        {
            if (view[i] == '.')
                ++num_prefix_components;
        }

        size_t total_num_components = ipV6 ? 8 : 4;
        if (num_prefix_components > total_num_components - 1)
            return {};

        String prefix(pattern, 0, first_wildcard);
        String expected_wildcard = "%";
        for (size_t i = 0; i != total_num_components - 1 - num_prefix_components; ++i)
        {
            prefix += ".0";
            expected_wildcard += ".%";
        }

        if (pattern.compare(first_wildcard, String::npos, expected_wildcard) != 0)
            return {};

        result.addIPSubnet(Poco::Net::IPAddress(prefix), num_prefix_components * 8);
        return result;
    }


    AllowedHosts extractAllowedHostsFromIPv4(const String & pattern)
    {
        return extractAllowedHostsFromIP<false>(pattern);
    }


    AllowedHosts extractAllowedHostsFromIPv6(const String & pattern)
    {
        return extractAllowedHostsFromIP<true>(pattern);
    }


    AllowedHosts extractAllowedHostsFromHostName(const String & pattern)
    {
        AllowedHosts result;
        size_t first_wildcard = pattern.find_first_of("%_");
        if (first_wildcard == String::npos)
        {
            result.addHostName(pattern);
            return result;
        }

        result.addHostRegexp(likePatternToRegexp(pattern));
        return result;
    }


    AllowedHosts extractAllowedHosts(const String & pattern)
    {
        if (!pattern.empty() && ((pattern[0] == '[') || (std::count(pattern.begin(), pattern.end(), ':') == 7)))
            return extractAllowedHostsFromIPv6(pattern);

        /// If `host` starts with digits and a dot then it's an IP pattern, otherwise it's a hostname pattern.
        size_t first_not_digit = pattern.find_first_not_of("0123456789");
        if ((first_not_digit != String::npos) && (first_not_digit != 0) && (pattern[first_not_digit] == '.'))
            return extractAllowedHostsFromIPv4(pattern);

        return extractAllowedHostsFromHostName(pattern);
    }


    bool parseNameImpl(IParser::Pos & pos, Expected & expected, String & result, AllowedHosts * allowed_hosts)
    {
        String name;
        if (!parseIdentifierOrStringLiteral(pos, expected, name))
            return false;

        if (ParserToken{TokenType::At}.ignore(pos, expected))
        {
            String host;
            if (!parseIdentifierOrStringLiteral(pos, expected, name))
                return false;

            if (host.empty())
                result = name;
            else
                result = name + "@" + host;

            if (allowed_hosts)
            {
                allowed_hosts->clear();
                try
                {
                    *allowed_hosts = extractAllowedHosts(host);
                }
                catch (...)
                {
                }
            }
        }
        else
        {
            result = name;
        }

        return true;
    }
}


bool parseUserName(IParser::Pos & pos, Expected & expected, String & result)
{
    return parseNameImpl(pos, expected, result, nullptr);
}


bool parseUserName(IParser::Pos & pos, Expected & expected, String & result, HostPatternExtractedFromUserName & host_pattern)
{
    return parseNameImpl(pos, expected, result, &host_pattern);
}


bool parseRoleName(IParser::Pos & pos, Expected & expected, String & result)
{
    return parseNameImpl(pos, expected, result, nullptr);
}
}
