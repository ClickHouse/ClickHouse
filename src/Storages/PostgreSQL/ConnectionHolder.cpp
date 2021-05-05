#include <Storages/PostgreSQL/ConnectionHolder.h>
#include <common/logger_useful.h>
#include <IO/Operators.h>


namespace postgres
{

String formatConnectionString(String dbname, String host, UInt16 port, String user, String password)
{
    DB::WriteBufferFromOwnString out;
    out << "dbname=" << DB::quote << dbname
        << " host=" << DB::quote << host
        << " port=" << port
        << " user=" << DB::quote << user
        << " password=" << DB::quote << password;
    return out.str();
}

ConnectionHolder::ConnectionHolder(const String & connection_string_, PoolPtr pool_, size_t pool_wait_timeout_)
    : connection_string(connection_string_), pool(pool_), pool_wait_timeout(pool_wait_timeout_) {}

ConnectionHolder::~ConnectionHolder()
{
    if (connection)
        pool->returnObject(std::move(connection));
}

pqxx::connection & ConnectionHolder::get()
{
    if (!connection)
    {
        pool->tryBorrowObject(connection, [&]()
        {
            auto new_connection = std::make_unique<pqxx::connection>(connection_string);
            LOG_DEBUG(&Poco::Logger::get("PostgreSQLConnection"),
                      "New connection to {}:{}", new_connection->hostname(), new_connection->port());
            return new_connection;
        }, pool_wait_timeout);
    }

    if (!connection->is_open())
    {
        connection = std::make_unique<pqxx::connection>(connection_string);
    }

    return *connection;
}

bool ConnectionHolder::isConnected()
{
    try
    {
        get();
    }
    catch (const pqxx::broken_connection & pqxx_error)
    {
        LOG_ERROR(
            &Poco::Logger::get("PostgreSQLConnection"),
            "Connection error: {}", pqxx_error.what());
        return false;
    }
    catch (...)
    {
        throw;
    }

    return true;
}
}
