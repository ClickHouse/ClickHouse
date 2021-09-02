#pragma once

#include <common/logger_useful.h>
#include <nanodbc/nanodbc.h>
#include <mutex>
#include <common/BorrowedObjectPool.h>
#include <unordered_map>

namespace DB
{
namespace ErrorCodes
{
    extern const int NO_FREE_CONNECTION;
}
}

namespace nanodbc
{

using ConnectionPtr = std::unique_ptr<nanodbc::connection>;
using Pool = BorrowedObjectPool<ConnectionPtr>;
using PoolPtr = std::shared_ptr<Pool>;

static constexpr inline auto ODBC_CONNECT_TIMEOUT = 100;


class ConnectionHolder
{
public:
    ConnectionHolder(PoolPtr pool_,
                     ConnectionPtr connection_,
                     const String & connection_string_)
        : pool(pool_)
        , connection(std::move(connection_))
        , connection_string(connection_string_)
    {
    }

    ConnectionHolder(const ConnectionHolder & other) = delete;

    ~ConnectionHolder()
    {
        pool->returnObject(std::move(connection));
    }

    nanodbc::connection & get() const
    {
        assert(connection != nullptr);
        return *connection;
    }

    void updateConnection()
    {
        connection = std::make_unique<nanodbc::connection>(connection_string, ODBC_CONNECT_TIMEOUT);
    }

private:
    PoolPtr pool;
    ConnectionPtr connection;
    const String & connection_string;
};

using ConnectionHolderPtr = std::shared_ptr<ConnectionHolder>;

}


namespace DB
{

static constexpr inline auto ODBC_CONNECT_TIMEOUT = 100;
static constexpr inline auto ODBC_POOL_WAIT_TIMEOUT = 10000;

template <typename T>
T execute(nanodbc::ConnectionHolderPtr connection_holder, std::function<T(nanodbc::connection &)> query_func)
{
    try
    {
        return query_func(connection_holder->get());
    }
    catch (const nanodbc::database_error & e)
    {
        /// SQLState, connection related errors start with 08S0.
        if (e.state().starts_with("08S0"))
        {
            connection_holder->updateConnection();
            return query_func(connection_holder->get());
        }
        throw;
    }
}


class ODBCConnectionFactory final : private boost::noncopyable
{
public:
    static ODBCConnectionFactory & instance()
    {
        static ODBCConnectionFactory ret;
        return ret;
    }

    nanodbc::ConnectionHolderPtr get(const std::string & connection_string, size_t pool_size)
    {
        std::lock_guard lock(mutex);

        if (!factory.count(connection_string))
            factory.emplace(std::make_pair(connection_string, std::make_shared<nanodbc::Pool>(pool_size)));

        auto & pool = factory[connection_string];

        nanodbc::ConnectionPtr connection;
        auto connection_available = pool->tryBorrowObject(connection, []() { return nullptr; }, ODBC_POOL_WAIT_TIMEOUT);

        if (!connection_available)
            throw Exception("Unable to fetch connection within the timeout", ErrorCodes::NO_FREE_CONNECTION);

        try
        {
            if (!connection)
                connection = std::make_unique<nanodbc::connection>(connection_string, ODBC_CONNECT_TIMEOUT);
        }
        catch (...)
        {
            pool->returnObject(std::move(connection));
            throw;
        }

        return std::make_unique<nanodbc::ConnectionHolder>(factory[connection_string], std::move(connection), connection_string);
    }

private:
    /// [connection_settings_string] -> [connection_pool]
    using PoolFactory = std::unordered_map<std::string, nanodbc::PoolPtr>;
    PoolFactory factory;
    std::mutex mutex;
};

}
