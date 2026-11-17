#pragma once

#include <string>
#include <utility>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

struct ProxyConfiguration
{
    enum class Protocol : uint8_t
    {
        HTTP,
        HTTPS
    };

    static auto protocolFromString(const std::string & str)
    {
        if (str == "http")
        {
            return Protocol::HTTP;
        }
        if (str == "https")
        {
            return Protocol::HTTPS;
        }

        if (str.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty protocol in the URL");

        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown protocol in the URL: {}", str);
    }

    static auto protocolToString(Protocol protocol)
    {
        switch (protocol)
        {
            case Protocol::HTTP:
                return "http";
            case Protocol::HTTPS:
                return "https";
        }
    }

    /// Splits a URI userinfo component ("user" or "user:password") into its two parts.
    /// Splits on the FIRST colon only, because a password may legally contain ':'.
    static std::pair<std::string, std::string> parseUserInfo(const std::string & user_info)
    {
        const auto pos = user_info.find(':');

        if (pos == std::string::npos)
        {
            return {user_info, {}};
        }

        return {user_info.substr(0, pos), user_info.substr(pos + 1)};
    }

    static bool useTunneling(Protocol request_protocol, Protocol proxy_protocol, bool disable_tunneling_for_https_requests_over_http_proxy)
    {
        bool is_https_request_over_http_proxy = request_protocol == Protocol::HTTPS && proxy_protocol == Protocol::HTTP;
        return is_https_request_over_http_proxy && !disable_tunneling_for_https_requests_over_http_proxy;
    }

    std::string host = std::string{};
    Protocol protocol = Protocol::HTTP;
    uint16_t port = 0;
    bool tunneling = false;
    Protocol original_request_protocol = Protocol::HTTP;
    std::string no_proxy_hosts = std::string{};
    std::string username = std::string{};
    std::string password = std::string{};

    bool isEmpty() const { return host.empty(); }
};

}
