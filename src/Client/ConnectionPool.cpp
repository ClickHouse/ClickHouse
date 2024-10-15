#include <Client/ConnectionPool.h>
#include <Core/Settings.h>

#include <boost/functional/hash.hpp>

namespace DB
{
namespace Setting
{
    extern const SettingsMilliseconds connection_pool_max_wait_ms;
}

ConnectionPoolPtr ConnectionPoolFactory::get(
    unsigned max_connections,
    String host,
    UInt16 port,
    String default_database,
    String user,
    String password,
    String proto_send_chunked,
    String proto_recv_chunked,
    String quota_key,
    String cluster,
    String cluster_secret,
    String client_name,
    Protocol::Compression compression,
    Protocol::Secure secure,
    Priority priority)
{
    Key key{
        max_connections, host, port, default_database, user, password, proto_send_chunked, proto_recv_chunked, quota_key, cluster, cluster_secret, client_name, compression, secure, priority};

    std::lock_guard lock(mutex);
    auto [it, inserted] = pools.emplace(key, ConnectionPoolPtr{});
    if (!inserted)
        if (auto res = it->second.lock())
            return res;

    ConnectionPoolPtr ret
    {
        new ConnectionPool(
            max_connections,
            host,
            port,
            default_database,
            user,
            password,
            proto_send_chunked,
            proto_recv_chunked,
            quota_key,
            cluster,
            cluster_secret,
            client_name,
            compression,
            secure,
            priority),
        [key, this](auto ptr)
        {
            {
                std::lock_guard another_lock(mutex);
                pools.erase(key);
            }
            delete ptr;
        }
    };
    it->second = ConnectionPoolWeakPtr(ret);
    return ret;
}

size_t ConnectionPoolFactory::KeyHash::operator()(const ConnectionPoolFactory::Key & k) const
{
    using boost::hash_combine;
    using boost::hash_value;
    size_t seed = 0;
    hash_combine(seed, hash_value(k.max_connections));
    hash_combine(seed, hash_value(k.host));
    hash_combine(seed, hash_value(k.port));
    hash_combine(seed, hash_value(k.default_database));
    hash_combine(seed, hash_value(k.user));
    hash_combine(seed, hash_value(k.password));
    hash_combine(seed, hash_value(k.cluster));
    hash_combine(seed, hash_value(k.cluster_secret));
    hash_combine(seed, hash_value(k.client_name));
    hash_combine(seed, hash_value(k.compression));
    hash_combine(seed, hash_value(k.secure));
    hash_combine(seed, hash_value(k.priority.value));
    return seed;
}


ConnectionPoolFactory & ConnectionPoolFactory::instance()
{
    static ConnectionPoolFactory ret;
    return ret;
}

IConnectionPool::Entry ConnectionPool::get(const DB::ConnectionTimeouts& timeouts, const DB::Settings& settings,
        bool force_connected)
{
    Entry entry = Base::get(settings[Setting::connection_pool_max_wait_ms].totalMilliseconds());

    if (force_connected)
        entry->forceConnected(timeouts);

    return entry;
}

}
