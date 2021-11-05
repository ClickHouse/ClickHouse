#pragma once

#include <Server/KeeperTCPHandler.h>

namespace DB
{
class KeeperTCPHandlers
{
public:
    static void registerConnection(KeeperTCPHandler * conn);
    static void unregisterConnection(KeeperTCPHandler * conn);
    static void dumpConnections(WriteBufferFromOwnString & buf, bool brief);

private:
    static std::mutex conns_mutex;
    static std::unordered_set<KeeperTCPHandler *> connections;
};
}
