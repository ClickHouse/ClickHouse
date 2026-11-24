#include <Storages/Pulsar/Client/ConnectionRegistry.h>

#include <algorithm>

#include <Storages/Pulsar/Client/Connection.h>

namespace DB::Pulsar
{

void ConnectionRegistry::add(const std::shared_ptr<Connection> & connection)
{
    std::lock_guard lock(mutex);
    connections.erase(
        std::remove_if(connections.begin(), connections.end(), [](const std::weak_ptr<Connection> & c) { return c.expired(); }),
        connections.end());
    connections.push_back(connection);
}

void ConnectionRegistry::closeAll()
{
    std::lock_guard lock(mutex);
    for (const auto & weak : connections)
        if (auto connection = weak.lock())
            connection->close();
    connections.clear();
}

}
