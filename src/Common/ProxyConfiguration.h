#pragma once

#include <string>
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

        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown proxy protocol: {}", str);
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

    bool isEmpty() const { return host.empty(); }
};

}
