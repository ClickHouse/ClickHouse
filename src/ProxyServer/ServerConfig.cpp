#include "ServerConfig.h"

#include <boost/functional/hash.hpp>

namespace Proxy
{

size_t ServerConfig::id() const
{
    size_t seed = 0;
    boost::hash_combine(seed, host);
    boost::hash_combine(seed, tcp_port);
    return seed;
}

bool ServerConfig::operator==(const ServerConfig & other) const
{
    return host == other.host && tcp_port == other.tcp_port;
}

}

namespace std
{
size_t hash<Proxy::ServerConfig>::operator()(const Proxy::ServerConfig & config) const
{
    return config.id();
}
}
