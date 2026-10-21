#pragma once

#include <functional>
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

/// Thread-safe wrapper that handles lazy initialization and automatic reconnection on session expiry.
/// The client is created lazily on first use to avoid blocking server startup with synchronous
/// Keeper session creation, which can time out if the leader is not yet available.
class KeeperHTTPClient
{
public:
    using ClientFactory = std::function<std::shared_ptr<zkutil::ZooKeeper>()>;

    explicit KeeperHTTPClient(ClientFactory factory_)
        : factory(std::move(factory_))
        , log(getLogger("KeeperHTTPClient")) {}

    /// Returns current client, creating it on first use or reconnecting if session has expired
    std::shared_ptr<zkutil::ZooKeeper> get()
    {
        std::lock_guard lock(mutex);
        if (!client)
            client = factory();
        else if (client->expired())
            client = client->startNewSession();
        return client;
    }

private:
    std::mutex mutex;
    ClientFactory factory;
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
