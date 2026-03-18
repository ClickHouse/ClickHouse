#include <Server/ConnectionRegistry.h>

#include <ctime>


namespace DB
{

ConnectionRegistry & ConnectionRegistry::instance()
{
    static ConnectionRegistry registry;
    return registry;
}

void ConnectionRegistry::enable()
{
    enabled.store(true, std::memory_order_relaxed);
}

ConnectionRegistry::Handle ConnectionRegistry::add(ConnectionInfo info)
{
    if (!enabled.load(std::memory_order_relaxed))
        return Handle(); /// no-op handle when feature is disabled

    UInt64 id = next_id.fetch_add(1, std::memory_order_relaxed);
    info.connection_id = id;
    if (info.connected_time == 0)
        info.connected_time = std::time(nullptr);

    std::unique_lock lock(mutex);
    connections.emplace(id, std::move(info));
    return Handle(*this, id);
}

std::vector<ConnectionInfo> ConnectionRegistry::list() const
{
    std::shared_lock lock(mutex);
    std::vector<ConnectionInfo> result;
    result.reserve(connections.size());
    for (const auto & [_, info] : connections)
        result.push_back(info);
    return result;
}

void ConnectionRegistry::remove(UInt64 id)
{
    std::unique_lock lock(mutex);
    connections.erase(id);
}

void ConnectionRegistry::update(UInt64 id, const String & status, const String & query_id, time_t last_query_time)
{
    std::unique_lock lock(mutex);
    auto it = connections.find(id);
    if (it == connections.end())
        return;
    it->second.status = status;
    it->second.query_id = query_id;
    if (last_query_time != 0)
        it->second.last_query_time = last_query_time;
}

void ConnectionRegistry::Handle::setActive(const String & query_id_)
{
    if (registry)
        registry->update(id, "active", query_id_, std::time(nullptr));
}

void ConnectionRegistry::Handle::setIdle()
{
    if (registry)
        registry->update(id, "idle", "", 0);
}

}
