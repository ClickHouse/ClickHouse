#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>
#include <Coordination/KeeperDispatcher.h>

namespace DB
{

class Context;
class IServer;

/// Replies "Ok.\n" if join request was successfully put info join_cluster_queue
template <class ContextWithKeeperDispatcherPtr>
class JoinClusterHandler : public HTTPRequestHandler
{
public:
    explicit JoinClusterHandler(IServer & server_, ContextWithKeeperDispatcherPtr context_)
        : server(server_), context(context_)
    {
    }

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;

private:
    IServer & server;
    ContextWithKeeperDispatcherPtr context;
};

}
