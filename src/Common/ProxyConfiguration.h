#pragma once

#include <string>

namespace DB
{

struct ProxyConfiguration
{
    enum class Protocol
    {
        HTTP,
        HTTPS,
        ANY
    };

    // TODO rename protocolFromString
    static auto fromString(const std::string & str)
    {
        if (str == "http")
        {
            return Protocol::HTTP;
        }
        else if (str == "https")
        {
            return Protocol::HTTPS;
        }
        else
        {
            return Protocol::ANY;
        }
    }

    static auto toString(Protocol protocol)
    {
        switch (protocol)
        {
            case Protocol::HTTP:
                return "http";
            case Protocol::HTTPS:
                return "https";
            case Protocol::ANY:
                return "any";
        }
    }

    std::string host;
    Protocol protocol;
    uint16_t port;
};

}
