#pragma once

#include <cstdint>
#include "config.h"

#if USE_NURAFT

#include <Common/ZooKeeper/KeeperOverDispatcher.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Coordination/KeeperDispatcher.h>
#include <Server/HTTP/HTTPRequestHandler.h>

namespace DB
{

class IServer;

/// Response with the storage info or perform an action on a given node from request
class KeeperHTTPStorageHandler : public HTTPRequestHandler
{
public:
    KeeperHTTPStorageHandler(const IServer & server_, std::shared_ptr<KeeperDispatcher> keeper_dispatcher_);

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;

private:
    void performZooKeeperRequest(
        Coordination::OpNum opnum, const std::string & storage_path, HTTPServerRequest & request, HTTPServerResponse & response) const;

    void performZooKeeperExistsRequest(const std::string & storage_path, HTTPServerResponse & response) const;
    void performZooKeeperListRequest(const std::string & storage_path, HTTPServerResponse & response) const;
    void performZooKeeperGetRequest(const std::string & storage_path, HTTPServerResponse & response) const;
    void performZooKeeperSetRequest(const std::string & storage_path, HTTPServerRequest & request, HTTPServerResponse & response) const;
    void performZooKeeperCreateRequest(const std::string & storage_path, HTTPServerRequest & request, HTTPServerResponse & response) const;
    void performZooKeeperRemoveRequest(const std::string & storage_path, HTTPServerRequest & request, HTTPServerResponse & response) const;

    LoggerPtr log;
    const IServer & server;
    std::shared_ptr<KeeperDispatcher> keeper_dispatcher;
    std::shared_ptr<zkutil::ZooKeeper> keeper_client;
    Poco::Timespan session_timeout;
    Poco::Timespan operation_timeout;
};

}
#endif
