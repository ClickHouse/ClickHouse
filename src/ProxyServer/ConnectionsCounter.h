#pragma once

#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include <ProxyServer/ServerConfig.h>

namespace Proxy
{

class GlobalConnectionsCounter
{
public:
    explicit GlobalConnectionsCounter();

    void updateConnectionCount(const ServerConfig & server, int64_t diff);

private:
    std::unordered_map<ServerConfig, size_t> counter;
};

class ConnectionsCounter
{
private:
    struct Entry
    {
        ServerConfig server;
        size_t count = 0;

        bool operator<(const Entry & other) const;
    };

public:
    explicit ConnectionsCounter(const std::vector<ServerConfig> & servers_ = {}, GlobalConnectionsCounter * global_counter_ = nullptr);

    ConnectionsCounter(ConnectionsCounter && other) noexcept;

    ConnectionsCounter & operator=(ConnectionsCounter && other) noexcept;

    void addConnection(const ServerConfig & server);

    void removeConnection(const ServerConfig & server);

    std::optional<std::string> getLeastLoaded() const;

    size_t size() const;

    bool empty() const;

    std::string operator[](size_t index) const;

private:
    void updateConnectionCount(const ServerConfig & server, int64_t diff);

    std::vector<ServerConfig> servers;
    std::unordered_map<ServerConfig, size_t> connection_count;
    std::set<Entry> least_loaded_servers;
    GlobalConnectionsCounter * global_counter;
    mutable std::mutex mutex;
};

}
