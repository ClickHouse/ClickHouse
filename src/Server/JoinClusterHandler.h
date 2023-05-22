#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>
#include <Coordination/KeeperDispatcher.h>

namespace DB
{

class Context;
class IServer;

/// Replies "Ok.\n" if join request was successfully put info join_cluster_queue
class JoinClusterHandler : public HTTPRequestHandler
{
public:
    explicit JoinClusterHandler(IServer & server_, std::shared_ptr<KeeperDispatcher> & keeper_dispatcher_)
        : server(server_), keeper_dispatcher(keeper_dispatcher_)
    {
    }

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;

private:
    IServer & server;
    std::shared_ptr<KeeperDispatcher> keeper_dispatcher;
};

}
