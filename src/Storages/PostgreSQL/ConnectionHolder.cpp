#include <Storages/PostgreSQL/ConnectionHolder.h>
#include <common/logger_useful.h>

namespace postgres
{

ConnectionHolder::ConnectionHolder(const String & connection_string_, PoolPtr pool_, size_t pool_wait_timeout)
    : connection_string(connection_string_), pool(pool_)
{
    pool->tryBorrowObject(connection, [&]()
    {
        auto new_connection = std::make_unique<pqxx::connection>(connection_string);
        LOG_DEBUG(&Poco::Logger::get("PostgreSQLConnection"),
                    "New connection to {}:{}", new_connection->hostname(), new_connection->port());
        return new_connection;
    }, pool_wait_timeout);
}

ConnectionHolder::~ConnectionHolder()
{
    if (connection)
        pool->returnObject(std::move(connection));
}

pqxx::connection & ConnectionHolder::get()
{
    if (!connection->is_open())
        connection = std::make_unique<pqxx::connection>(connection_string);

    return *connection;
}

}
