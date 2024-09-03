#pragma once

#include <cstdint>
#include <config.h>

#if USE_NURAFT

#include <Coordination/KeeperDispatcher.h>
#include <Server/HTTP/HTTPRequestHandler.h>
#include <Server/HTTP/HTTPRequestHandlerFactory.h>
#include <Server/HTTPPathHints.h>

namespace DB
{

class IServer;

/// Handle request using child handlers
class KeeperHTTPRequestHandlerFactory : public HTTPRequestHandlerFactory
{
public:
    explicit KeeperHTTPRequestHandlerFactory(const std::string & name_);

    void addHandler(HTTPRequestHandlerFactoryPtr child_factory) { child_factories.emplace_back(child_factory); }

    void addPathToHints(const std::string & http_path) { hints.add(http_path); }

    std::unique_ptr<HTTPRequestHandler> createRequestHandler(const HTTPServerRequest & request) override;

private:
    LoggerPtr log;
    std::string name;
    HTTPPathHints hints;

    std::vector<HTTPRequestHandlerFactoryPtr> child_factories;
};

class KeeperHTTPReadinessHandler : public HTTPRequestHandler, WithContext
{
private:
    LoggerPtr log;
    std::shared_ptr<KeeperDispatcher> keeper_dispatcher;

public:
    explicit KeeperHTTPReadinessHandler(std::shared_ptr<KeeperDispatcher> keeper_dispatcher_);

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
};

class KeeperHTTPCommandsHandler : public HTTPRequestHandler
{
private:
    LoggerPtr log;
    std::shared_ptr<KeeperDispatcher> keeper_dispatcher;

public:
    explicit KeeperHTTPCommandsHandler(std::shared_ptr<KeeperDispatcher> keeper_dispatcher_);

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
};

HTTPRequestHandlerFactoryPtr createKeeperHTTPHandlerFactory(
    const IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    std::shared_ptr<KeeperDispatcher> keeper_dispatcher,
    const std::string & name);
}

#endif
