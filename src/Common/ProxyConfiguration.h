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
    enum class Protocol
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
        else if (str == "https")
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

    std::string host;
    Protocol protocol;
    uint16_t port;
    bool tunneling;
    Protocol original_request_protocol;
};

}
