#pragma once

#include "config.h"

#if USE_NURAFT

#include <Coordination/KeeperDispatcher.h>
#include <Server/HTTP/HTTPRequestHandler.h>

namespace DB
{

/// Response with HTML page that allows to send queries and show results in browser.
class KeeperDashboardWebUIRequestHandler : public HTTPRequestHandler
{
public:
    explicit KeeperDashboardWebUIRequestHandler() = default;
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
};

/// Response with json containing dashboard information to be displayed
class KeeperDashboardContentRequestHandler : public HTTPRequestHandler
{
private:
    std::shared_ptr<KeeperDispatcher> keeper_dispatcher;

public:
    explicit KeeperDashboardContentRequestHandler(std::shared_ptr<KeeperDispatcher> keeper_dispatcher_)
        : keeper_dispatcher(keeper_dispatcher_)
    {
    }
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
};

}
#endif
