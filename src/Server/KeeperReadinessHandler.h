#pragma once

#include <memory>
#include <Server/HTTP/HTTPRequestHandler.h>
#include <Server/HTTP/HTMLForm.h>
#include <Coordination/KeeperDispatcher.h>

namespace DB
{

class Context;
class IServer;

class KeeperReadinessHandler : public HTTPRequestHandler, WithContext
{
private:
    IServer & server;
    std::shared_ptr<KeeperDispatcher> keeper_dispatcher;

public:
    explicit KeeperReadinessHandler(IServer & server_, std::shared_ptr<KeeperDispatcher> keeper_dispatcher_)
        : server(server_)
        , keeper_dispatcher(keeper_dispatcher_)
    {
    }

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;
};


}
