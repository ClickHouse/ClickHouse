#include <Common/parseAddress.h>
#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <IO/ReadHelpers.h>
#include <base/find_symbols.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <algorithm>
#include <arpa/inet.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INVALID_CONFIG_PARAMETER;
}

std::pair<std::string, UInt16> parseAddress(const std::string & str, UInt16 default_port)
{
    if (str.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty address passed to function parseAddress");

    const char * begin = str.data();
    const char * end = begin + str.size();
    const char * port = end; // NOLINT

    if (begin[0] == '[')
    {
        const char * closing_square_bracket = find_first_symbols<']'>(begin + 1, end);
        if (closing_square_bracket >= end)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Illegal address passed to function parseAddress: "
                            "the address begins with opening square bracket, but no closing square bracket found");

        port = closing_square_bracket + 1;
    }
    else
        port = find_first_symbols<':'>(begin, end);

    if (port != end)
    {
        if (*port != ':')
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Illegal port prefix passed to function parseAddress: {}", port);

        ++port;

        UInt16 port_number = 0;
        ReadBufferFromMemory port_buf(port, end - port);
        if (!tryReadText(port_number, port_buf) || !port_buf.eof())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Illegal port passed to function parseAddress: {}", port);
        }
        return { std::string(begin, port - 1), port_number };
    }
    if (default_port)
    {
        return {str, default_port};
    }
    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "The address passed to function parseAddress doesn't contain port number and no 'default_port' was passed");
}

std::string getHostFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & key)
{
    std::string host = config.getString(key);

    const char * reason = nullptr;
    in_addr ipv4;
    if (host.empty())
        reason = "it is empty";
    else if (std::ranges::any_of(host, [](char c) { return isWhitespaceASCII(c) || isControlASCII(c) || c == '\x7f'; }))
        reason = "it contains whitespace or a control character";
    /// `DNSResolver` takes such a value for an IPv4 address through `inet_aton` too, so the short forms (`127.1`) stay valid.
    else if (host.find_first_not_of("0123456789.") == std::string::npos && !inet_aton(host.c_str(), &ipv4))
        reason = "it is not a valid IPv4 address";

    if (reason)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Invalid host '{}' in config key '{}': {}", host, key, reason);
    return host;
}

}
