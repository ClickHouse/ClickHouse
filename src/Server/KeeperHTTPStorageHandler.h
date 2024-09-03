#pragma once

#include <cstdint>
#include "config.h"

#if USE_NURAFT

#include <Coordination/KeeperDispatcher.h>
#include <Server/HTTP/HTTPRequestHandler.h>

namespace DB
{

class IServer;

/// Response with the storage info or perform an action on a given node from request
class KeeperHTTPStorageHandler : public HTTPRequestHandler
{
private:
    LoggerPtr log;
    const IServer & server;
    std::shared_ptr<KeeperDispatcher> keeper_dispatcher;
    Poco::Timespan session_timeout;
    Poco::Timespan operation_timeout;

public:
    KeeperHTTPStorageHandler(const IServer & server_, std::shared_ptr<KeeperDispatcher> keeper_dispatcher_);

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;

private:
    Coordination::ZooKeeperResponsePtr awaitKeeperResponse(std::shared_ptr<Coordination::ZooKeeperRequest> request);

    void performZooKeeperRequest(
        Coordination::OpNum opnum, const std::string & storage_path, HTTPServerRequest & request, HTTPServerResponse & response);

    void performZooKeeperExistsRequest(const std::string & storage_path, HTTPServerResponse & response);
    void performZooKeeperListRequest(const std::string & storage_path, HTTPServerResponse & response);
    void performZooKeeperGetRequest(const std::string & storage_path, HTTPServerResponse & response);
    void performZooKeeperSetRequest(const std::string & storage_path, HTTPServerRequest & request, HTTPServerResponse & response);
    void performZooKeeperCreateRequest(const std::string & storage_path, HTTPServerRequest & request, HTTPServerResponse & response);
    void performZooKeeperRemoveRequest(const std::string & storage_path, HTTPServerRequest & request, HTTPServerResponse & response);
};

}
#endif
