#pragma once

#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include "PostgreSQLConnection.h"


namespace postgres
{

class PoolWithFailover;

/// Connection pool size is defined by user with setting `postgresql_connection_pool_size` (default 16).
/// If pool is empty, it will block until there are available connections.
/// If setting `connection_pool_wait_timeout` is defined, it will not block on empty pool and will
/// wait until the timeout and then create a new connection. (only for storage/db engine)
class ConnectionPool
{

friend class PoolWithFailover;

static constexpr inline auto POSTGRESQL_POOL_DEFAULT_SIZE = 16;

public:

    ConnectionPool(
        std::string dbname,
        std::string host,
        UInt16 port,
        std::string user,
        std::string password,
        size_t pool_size_ = POSTGRESQL_POOL_DEFAULT_SIZE,
        int64_t pool_wait_timeout_ = -1);

    ConnectionPool(const ConnectionPool & other);

    ConnectionPool operator =(const ConnectionPool &) = delete;

    ConnectionHolderPtr get();

private:
    using Pool = ConcurrentBoundedQueue<ConnectionPtr>;
    using PoolPtr = std::shared_ptr<Pool>;

    static std::string formatConnectionString(
        std::string dbname, std::string host, UInt16 port, std::string user, std::string password);

    void initialize();

    PoolPtr pool;
    std::string connection_str, address;
    size_t pool_size;
    int64_t pool_wait_timeout;
    bool block_on_empty_pool;
};

using ConnectionPoolPtr = std::shared_ptr<ConnectionPool>;

}

#endif
