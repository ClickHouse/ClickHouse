#pragma once

#include <list>
#include <memory>
#include <mutex>
#include <atomic>

#include <Poco/Exception.h>
#include <Poco/Logger.h>
#include <common/BorrowedObjectPool.h>

#include <mysqlxx/IPool.h>
#include <mysqlxx/Connection.h>

namespace mysqlxx
{

static constexpr size_t DEFAULT_MAX_CONNECTIONS = 16;

struct PoolConfiguration
{
    size_t max_connections = DEFAULT_MAX_CONNECTIONS;
    size_t get_connection_timeout_milliseconds = 5000;
    bool share_connection = false;
    bool close_connection = false;
};

class Pool final: public IPool, public std::enable_shared_from_this<Pool>
{
public:

    static std::shared_ptr<Pool> create(
        const ConnectionConfiguration & connection_configuration,
        const PoolConfiguration & pool_configuration = PoolConfiguration());

    IPool::Entry getEntry() override;

    IPool::Entry tryGetEntry(size_t timeout_in_milliseconds) override;

    const ConnectionConfiguration & getConnectionConfiguration() const
    {
        return connection_configuration;
    }

    const PoolConfiguration & getPoolConfiguration() const
    {
        return pool_configuration;
    }

private:
    friend class PoolFactory;

    static void resetShareConnectionPools();

    void returnConnectionToPool(mysqlxx::Connection && connection) override;

    explicit Pool(
        const ConnectionConfiguration & connection_configuration_,
        const PoolConfiguration & pool_configuration_);

    const ConnectionConfiguration connection_configuration;
    PoolConfiguration pool_configuration;
    BorrowedObjectPool<mysqlxx::Connection> connections_object_pool;
    Poco::Logger & logger;

};

}
