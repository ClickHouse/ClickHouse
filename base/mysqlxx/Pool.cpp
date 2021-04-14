#if __has_include(<mysql.h>)
#include <mysql.h>
#include <mysqld_error.h>
#else
#include <mysql/mysql.h>
#include <mysql/mysqld_error.h>
#endif

#include <mysqlxx/Pool.h>

namespace
{
    std::mutex mutex;
    std::unordered_map<std::string, std::shared_ptr<mysqlxx::Pool>> connection_identity_to_pool;

    std::shared_ptr<mysqlxx::Pool> getFromCache(const std::string & connection_identity)
    {
        std::lock_guard<std::mutex> lock(mutex);

        auto it = connection_identity_to_pool.find(connection_identity);
        if (it == connection_identity_to_pool.end())
            return nullptr;

        return it->second;
    }

    void putInCache(const std::string & connection_identity, std::shared_ptr<mysqlxx::Pool> pool)
    {
        std::lock_guard<std::mutex> lock(mutex);
        connection_identity_to_pool[connection_identity] = std::move(pool);
    }

    void clearCache()
    {
        connection_identity_to_pool.clear();
    }
}

namespace mysqlxx
{


std::shared_ptr<Pool> Pool::create(
    const ConnectionConfiguration & connection_configuration,
    const PoolConfiguration & pool_configuration)
{
    std::string connection_identity;
    bool share_connection = pool_configuration.share_connection;

    if (share_connection)
    {
        connection_identity = connection_configuration.getIdentity();
        auto result = getFromCache(connection_identity);

        if (result != nullptr)
            return result;
    }

    std::unique_ptr<Pool> pool(new Pool(connection_configuration, pool_configuration));
    std::shared_ptr<Pool> pool_ptr(std::move(pool));

    if (share_connection)
        putInCache(connection_identity, pool_ptr);

    return pool_ptr;
}

void Pool::resetShareConnectionPools()
{
    clearCache();
}

Pool::Pool(
    const ConnectionConfiguration & connection_configuration_,
    const PoolConfiguration & pool_configuration_)
    : connection_configuration(connection_configuration_)
    , pool_configuration(pool_configuration_)
    , connections_object_pool(pool_configuration.max_connections)
    , logger(Poco::Logger::get("mysqlxx::Pool"))
{
}

Pool::Entry Pool::getEntry()
{
    mysqlxx::Connection connection;
    bool get_connection_result = connections_object_pool.tryBorrowObject(connection, []() { return mysqlxx::Connection(); }, pool_configuration.get_connection_timeout_milliseconds);
    if (!get_connection_result)
        throw Poco::Exception("Pool ({}) get connection timeout ({}) exceeded", getConnectionConfiguration().getDescription(), pool_configuration.get_connection_timeout_milliseconds);

    if (!connection.isConnected())
    {
        try
        {
            connection.connect(connection_configuration);
        }
        catch (...)
        {
            connections_object_pool.returnObject(std::move(connection));
            throw;
        }
    }

    auto connection_holder = std::make_shared<IPool::ConnectionHolder>(std::move(connection), shared_from_this());

    IPool::Entry result;
    result.connection_holder = std::move(connection_holder);

    return result;
}

Pool::Entry Pool::tryGetEntry(size_t timeout_in_milliseconds)
{
    IPool::Entry result;
    mysqlxx::Connection connection;
    bool get_connection_result = connections_object_pool.tryBorrowObject(connection, []() { return mysqlxx::Connection(); }, timeout_in_milliseconds);

    if (!get_connection_result)
        return result;

    if (!connection.isConnected())
    {
        bool connect_result = connection.tryConnect(connection_configuration);
        if (!connect_result)
        {
            connections_object_pool.returnObject(std::move(connection));
            return result;
        }
    }

    auto connection_holder = std::make_shared<IPool::ConnectionHolder>(std::move(connection), shared_from_this());
    result.connection_holder = std::move(connection_holder);

    return result;
}

void Pool::returnConnectionToPool(mysqlxx::Connection && connection)
{
    if (pool_configuration.close_connection)
        connection.disconnect();

    connections_object_pool.returnObject(std::move(connection));
}

}
