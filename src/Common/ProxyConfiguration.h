#pragma once

#include <string>

namespace DB
{

struct ProxyConfiguration
{
    std::string host;
    std::string protocol;
    uint16_t port;
};

}
