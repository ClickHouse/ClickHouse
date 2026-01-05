#pragma once

#include <config.h>

#if USE_NURAFT

#include <Common/ZooKeeper/KeeperOverDispatcher.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Coordination/KeeperDispatcher.h>
#include <Server/HTTP/HTTPRequestHandler.h>
#include <Server/HTTP/HTTPRequestHandlerFactory.h>
#include <Server/HTTPPathHints.h>
#include <Server/KeeperHTTPStorageHandler.h>

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
public:
    explicit KeeperHTTPReadinessHandler(std::shared_ptr<KeeperDispatcher> keeper_dispatcher_);

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;

private:
    LoggerPtr log;
    std::shared_ptr<KeeperDispatcher> keeper_dispatcher;
};

class KeeperHTTPCommandsHandler : public HTTPRequestHandler
{
public:
    KeeperHTTPCommandsHandler(
        std::shared_ptr<KeeperDispatcher> keeper_dispatcher_,
        std::shared_ptr<KeeperHTTPClient> keeper_client_);

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;

private:
    LoggerPtr log;
    std::shared_ptr<KeeperDispatcher> keeper_dispatcher;
    std::shared_ptr<KeeperHTTPClient> keeper_client;
};

HTTPRequestHandlerFactoryPtr createKeeperHTTPHandlerFactory(
    const IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    std::shared_ptr<KeeperDispatcher> keeper_dispatcher,
    const std::string & name);
}

#endif
