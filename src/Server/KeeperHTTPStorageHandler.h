#pragma once

#include <cstdint>
#include <optional>
#include "config.h"

#if USE_NURAFT

#include <mutex>

#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/logger_useful.h>
#include <Server/HTTP/HTTPRequestHandler.h>

namespace DB
{

class IServer;

/// Thread-safe wrapper that handles automatic reconnection on session expiry
class KeeperHTTPClient
{
public:
    explicit KeeperHTTPClient(std::shared_ptr<zkutil::ZooKeeper> client_)
        : client(std::move(client_))
        , log(getLogger("KeeperHTTPClient")) {}

    /// Returns current client, reconnecting if session has expired
    std::shared_ptr<zkutil::ZooKeeper> get()
    {
        std::lock_guard lock(mutex);
        LOG_TRACE(log, "get() called, session={}, expired={}", client->getClientID(), client->expired());
        if (client->expired())
            client = client->startNewSession();
        return client;
    }

private:
    std::mutex mutex;
    std::shared_ptr<zkutil::ZooKeeper> client;
    LoggerPtr log;
};

/// Response with the storage info or perform an action on a given node from request
class KeeperHTTPStorageHandler : public HTTPRequestHandler
{
public:
    KeeperHTTPStorageHandler(
        std::shared_ptr<KeeperHTTPClient> keeper_client_,
        size_t max_request_size_);

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;

private:
    static std::optional<Coordination::OpNum> getOperationFromRequest(
        const HTTPServerRequest & request, HTTPServerResponse & response);

    void performZooKeeperRequest(
        Coordination::OpNum opnum, const std::string & storage_path, HTTPServerRequest & request, HTTPServerResponse & response) const;

    void performZooKeeperExistsRequest(const std::string & storage_path, HTTPServerResponse & response) const;
    void performZooKeeperListRequest(const std::string & storage_path, HTTPServerResponse & response) const;
    void performZooKeeperGetRequest(const std::string & storage_path, HTTPServerResponse & response) const;
    void performZooKeeperSetRequest(const std::string & storage_path, HTTPServerRequest & request, HTTPServerResponse & response) const;
    void performZooKeeperCreateRequest(const std::string & storage_path, HTTPServerRequest & request, HTTPServerResponse & response) const;
    void performZooKeeperRemoveRequest(const std::string & storage_path, const HTTPServerRequest & request, HTTPServerResponse & response) const;

    LoggerPtr log;
    std::shared_ptr<KeeperHTTPClient> keeper_client;
    size_t max_request_size = 0;
};

}
#endif
