#pragma once

#include <string>

#include <base/types.h>

namespace Proxy
{

struct ServerConfig
{
    std::string key;
    std::string host;
    UInt16 tcp_port = 0;

    size_t id() const;

    bool operator==(const ServerConfig & other) const;
};

}

namespace std
{
template <>
struct hash<Proxy::ServerConfig>
{
    size_t operator()(const Proxy::ServerConfig & config) const;
};
}
