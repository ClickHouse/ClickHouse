#include <Server/KeeperTCPHandlers.h>

namespace DB
{
void KeeperTCPHandlers::registerConnection(KeeperTCPHandler * conn)
{
    std::lock_guard lock(conns_mutex);
    connections.insert(conn);
}

void KeeperTCPHandlers::unregisterConnection(KeeperTCPHandler * conn)
{
    std::lock_guard lock(conns_mutex);
    connections.erase(conn);
}

void KeeperTCPHandlers::dumpConnections(WriteBufferFromOwnString & buf, bool brief)
{
    std::lock_guard lock(conns_mutex);
    for (auto * conn : connections)
    {
        conn->dumpStats(buf, brief);
    }
}
}
