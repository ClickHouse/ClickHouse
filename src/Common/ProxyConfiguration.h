#pragma once

namespace DB
{

struct ProxyConfiguration
{
    std::string host;
    std::string scheme;
    uint16_t port;
};

}
