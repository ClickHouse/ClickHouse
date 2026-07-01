#pragma once

#include <string>

namespace Proxy
{

struct ServerConfig
{
    std::string key;
    std::string host;
    int tcp_port;

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
