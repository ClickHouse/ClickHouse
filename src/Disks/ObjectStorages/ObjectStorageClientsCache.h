#pragma once

#include <Disks/ObjectStorages/IObjectStorageConnectionInfo.h>
#include <IO/S3/Client.h>
#include <IO/S3/ProviderType.h>
#include <boost/core/noncopyable.hpp>

namespace CurrentMetrics
{
    extern const Metric DistrCacheServerS3CachedClients;
}

namespace ProfileEvents
{
    extern const Event DistrCacheServerNewS3CachedClients;
    extern const Event DistrCacheServerReusedS3CachedClients;
}

namespace DB
{

/// A cache for Object Storage clients.
/// All S3 clients are separated by users.
/// Users are associated with concrete ClickHouse services (shared among different replicas).
/// For each such user we store a set of clients.
/// Each client is uniqly identified by ObjectStorageClientInfo::Hash.
/// We use LRU to manage the number of clients.
template <typename T>
class ObjectStorageClientsCache : boost::noncopyable
{
public:
    using User = std::string;
    using ClientPtr = std::shared_ptr<typename T::Client>;
    using ClientInfoHash = UInt128;

    explicit ObjectStorageClientsCache(size_t max_clients_) : max_clients(max_clients_) { }

    static ObjectStorageClientsCache & instance()
    {
        static ObjectStorageClientsCache ret(/* max_clients */ 100);
        return ret;
    }

    std::shared_ptr<typename T::Client> getClient(const std::string & user, const typename T::ClientInfo & info)
    {
        std::lock_guard lock(mutex);

        SipHash hash;
        hash.update(user);
        info.updateHash(hash, /* include_credentials */true);
        auto client_hash = hash.get128();

        auto it = clients.find(client_hash);
        if (it == clients.end())
        {
            LOG_TEST(getLogger("ObjectStorageClientsCache"), "Total clients size: {}, adding client for user: {}", clients.size(), user);

            if (clients_lru.size() == max_clients)
            {
                auto client = clients_lru.front();
                clients.erase(client.hash);
                clients_lru.pop_front();
                chassert(clients_lru.size() < max_clients);
            }
            else
                CurrentMetrics::add(CurrentMetrics::DistrCacheServerS3CachedClients);

            ProfileEvents::increment(ProfileEvents::DistrCacheServerNewS3CachedClients);
            auto client = T::makeClient(info);
            clients_lru.emplace_back(client_hash, client);
            clients.emplace(client_hash, std::prev(clients_lru.end()));
            return client;
        }
        else
        {
            LOG_TEST(getLogger("ObjectStorageClientsCache"), "Total clients size: {}, reusing client for user: {}", clients.size(), user);

            ProfileEvents::increment(ProfileEvents::DistrCacheServerReusedS3CachedClients);
            clients_lru.splice(clients_lru.end(), clients_lru, it->second);
            return it->second->client;
        }
    }

private:
    struct ClientData
    {
        ClientData(const ClientInfoHash & hash_, ClientPtr client_) : hash(hash_), client(client_) { }
        ClientInfoHash hash;
        ClientPtr client;
    };
    using ClientsLRUQueue = std::list<ClientData>;

    const size_t max_clients;
    std::mutex mutex;
    std::unordered_map<ClientInfoHash, typename ClientsLRUQueue::iterator> clients;
    ClientsLRUQueue clients_lru;
};

}
