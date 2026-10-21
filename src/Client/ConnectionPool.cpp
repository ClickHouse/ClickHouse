#include <Client/ConnectionPool.h>
#include <Core/Settings.h>
#include <IO/WriteHelpers.h>

#include <boost/functional/hash.hpp>


namespace DB
{
namespace Setting
{
    extern const SettingsMilliseconds connection_pool_max_wait_ms;
}

IConnectionPool::IConnectionPool(String host_, UInt16 port_, Priority config_priority_)
    : host(host_), port(port_), address(host + ":" + toString(port_)), config_priority(config_priority_)
{
}

ConnectionPool::ConnectionPool(
    unsigned max_connections_,
    const String & host_,
    UInt16 port_,
    const String & default_database_,
    const String & user_,
    const String & password_,
    const String & proto_send_chunked_,
    const String & proto_recv_chunked_,
    const String & quota_key_,
    const String & cluster_,
    const String & cluster_secret_,
    const String & client_name_,
    Protocol::Compression compression_,
    Protocol::Secure secure_,
    const String & bind_host_,
    Priority config_priority_)
    : IConnectionPool(host_, port_, config_priority_)
    , Base(max_connections_, getLogger("ConnectionPool (" + host_ + ":" + toString(port_) + ")"))
    , default_database(default_database_)
    , user(user_)
    , password(password_)
    , proto_send_chunked(proto_send_chunked_)
    , proto_recv_chunked(proto_recv_chunked_)
    , quota_key(quota_key_)
    , cluster(cluster_)
    , cluster_secret(cluster_secret_)
    , client_name(client_name_)
    , compression(compression_)
    , secure(secure_)
    , bind_host(bind_host_)
{
}

std::string ConnectionPool::getDescription() const
{
    return host + ":" + toString(port);
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
    String bind_host,
    Priority priority)
{
    Key key{
        max_connections, host, port, default_database, user, password, proto_send_chunked, proto_recv_chunked, quota_key, cluster, cluster_secret, client_name, compression, secure, bind_host, priority};

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
           bind_host,
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
    hash_combine(seed, hash_value(k.bind_host));
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
