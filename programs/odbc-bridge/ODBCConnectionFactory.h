#pragma once

#include <common/logger_useful.h>
#include <nanodbc/nanodbc.h>
#include <mutex>
#include <common/BorrowedObjectPool.h>
#include <unordered_map>


namespace nanodbc
{

static constexpr inline auto ODBC_CONNECT_TIMEOUT = 100;

using ConnectionPtr = std::shared_ptr<nanodbc::connection>;
using Pool = BorrowedObjectPool<ConnectionPtr>;
using PoolPtr = std::shared_ptr<Pool>;

class ConnectionHolder
{

public:
    ConnectionHolder(const std::string & connection_string_, PoolPtr pool_) : connection_string(connection_string_), pool(pool_) {}

    ~ConnectionHolder()
    {
        if (connection)
            pool->returnObject(std::move(connection));
    }

    nanodbc::connection & operator*()
    {
        if (!connection)
        {
            pool->borrowObject(connection, [&]()
            {
                return std::make_shared<nanodbc::connection>(connection_string, ODBC_CONNECT_TIMEOUT);
            });
        }

        return *connection;
    }

private:
    std::string connection_string;
    PoolPtr pool;
    ConnectionPtr connection;
};

}


namespace DB
{

class ODBCConnectionFactory final : private boost::noncopyable
{
public:
    static ODBCConnectionFactory & instance()
    {
        static ODBCConnectionFactory ret;
        return ret;
    }

    nanodbc::ConnectionHolder get(const std::string & connection_string, size_t pool_size)
    {
        std::lock_guard lock(mutex);

        if (!factory.count(connection_string))
            factory.emplace(std::make_pair(connection_string, std::make_shared<nanodbc::Pool>(pool_size)));

        return nanodbc::ConnectionHolder(connection_string, factory[connection_string]);
    }

private:
    /// [connection_settings_string] -> [connection_pool]
    using PoolFactory = std::unordered_map<std::string, nanodbc::PoolPtr>;
    PoolFactory factory;
    std::mutex mutex;
};

}
