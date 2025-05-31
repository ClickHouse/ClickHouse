
#include "ConnectionsCounter.h"

#include <mutex>
#include <stdexcept>

#include <ProxyServer/ServerConfig.h>

namespace Proxy
{

GlobalConnectionsCounter::GlobalConnectionsCounter() = default;

void GlobalConnectionsCounter::updateConnectionCount(const ServerConfig & server, int64_t diff)
{
    auto [iter, inserted] = counter.emplace(server, 0);
    iter->second += diff;
    if (iter->second == 0)
    {
        counter.erase(iter);
    }
}

bool ConnectionsCounter::Entry::operator<(const Entry & other) const
{
    return count < other.count;
}

ConnectionsCounter::ConnectionsCounter(const std::vector<ServerConfig> & servers_, GlobalConnectionsCounter * global_counter_)
    : servers(servers_)
    , global_counter(global_counter_)
{
    connection_count.reserve(servers_.size());
    for (const auto & server : servers_)
    {
        if (global_counter_)
        {
            const auto iter = connection_count.find(server);
            connection_count[server] = iter == connection_count.end() ? 0 : iter->second;
        }
        else
        {
            connection_count[server] = 0;
        }
    }

    for (const auto & server : servers)
    {
        least_loaded_servers.insert({.server = server});
    }
}

ConnectionsCounter::ConnectionsCounter(ConnectionsCounter && other) noexcept
    : servers(std::move(other.servers))
    , connection_count(std::move(other.connection_count))
    , least_loaded_servers(std::move(other.least_loaded_servers))
    , global_counter(other.global_counter)
{
}

ConnectionsCounter & ConnectionsCounter::operator=(ConnectionsCounter && other) noexcept
{
    if (this != &other)
    {
        std::scoped_lock lock(mutex, other.mutex);
        servers = std::move(other.servers);
        connection_count = std::move(other.connection_count);
        least_loaded_servers = std::move(other.least_loaded_servers);
        global_counter = other.global_counter;
    }
    return *this;
}

void ConnectionsCounter::addConnection(const ServerConfig & server)
{
    updateConnectionCount(server, 1);
}

void ConnectionsCounter::removeConnection(const ServerConfig & server)
{
    updateConnectionCount(server, -1);
}

std::optional<std::string> ConnectionsCounter::getLeastLoaded() const
{
    std::lock_guard lock(mutex);
    auto it = least_loaded_servers.begin();
    return it == least_loaded_servers.end() ? std::nullopt : std::optional(it->server.key);
}

size_t ConnectionsCounter::size() const
{
    return servers.size();
}

bool ConnectionsCounter::empty() const
{
    return servers.empty();
}

std::string ConnectionsCounter::operator[](size_t index) const
{
    return servers[index].key;
}

void ConnectionsCounter::updateConnectionCount(const ServerConfig & server, int64_t diff)
{
    std::lock_guard lock(mutex);
    const auto it = connection_count.find(server);
    if (it == connection_count.end())
    {
        throw std::runtime_error("Server with key " + server.key + " not found in ConnectionsCounter");
    }
    least_loaded_servers.erase({.server = server, .count = it->second});
    (it->second) += diff;
    least_loaded_servers.insert({.server = server, .count = it->second});

    if (global_counter)
    {
        global_counter->updateConnectionCount(server, diff);
    }
}

}
