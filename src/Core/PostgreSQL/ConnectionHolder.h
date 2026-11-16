#pragma once

#include "config.h"

#if USE_LIBPQXX

#include <pqxx/pqxx>
#include <Core/Types.h>
#include <base/BorrowedObjectPool.h>
#include <Core/PostgreSQL/Connection.h>
#include <Common/logger_useful.h>

#include <atomic>


namespace postgres
{

using Pool = BorrowedObjectPool<ConnectionPtr>;
using PoolPtr = std::shared_ptr<Pool>;

class ConnectionHolder
{

public:
    ConnectionHolder(PoolPtr pool_, ConnectionPtr connection_, bool auto_close_)
        : pool(pool_)
        , connection(std::move(connection_))
        , auto_close(auto_close_)
    {}

    ConnectionHolder(const ConnectionHolder & other) = delete;

    void setBroken() { is_broken = true; }

    ~ConnectionHolder()
    {
        if (auto_close)
        {
            connection.reset();
        }
        else if (is_broken)
        {
            try
            {
                connection->getRef().reset();
            }
            catch (...)
            {
                connection.reset();
            }
        }
        pool->returnObject(std::move(connection));
    }

    pqxx::connection & get()
    {
        return connection->getRef();
    }

    void update()
    {
        connection->updateConnection();
    }

    /// Runs `func` on the connection. A connection that was idle in the pool may have been closed by
    /// the server (for example, when it was restarted), and libpq notices it only on the next query.
    /// So on `pqxx::broken_connection` reconnect and run `func` once more.
    /// Use only for `func` that is safe to repeat.
    template <typename Func>
    decltype(auto) execWithReconnect(Func && func)
    {
        try
        {
            return func(get());
        }
        catch (const pqxx::broken_connection & e)
        {
            LOG_DEBUG(getLogger("PostgreSQLConnectionHolder"), "Connection to {} is broken, reconnecting. (Message: {})",
                      connection->getInfoForLog(), e.what());
            update();
        }
        return func(get());
    }

private:
    PoolPtr pool;
    ConnectionPtr connection;
    bool auto_close;
    /// Written by the cancelling thread and by the thread that starts the read, which can race.
    std::atomic<bool> is_broken = false;
};

using ConnectionHolderPtr = std::unique_ptr<ConnectionHolder>;
}

#endif
