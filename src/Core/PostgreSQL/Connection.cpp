#include "Connection.h"
#include <common/logger_useful.h>

namespace postgres
{

Connection::Connection(const ConnectionInfo & connection_info_, bool replication_)
    : connection_info(connection_info_), replication(replication_)
{
    if (replication)
    {
        connection_info = std::make_pair(
            fmt::format("{} replication=database", connection_info.first), connection_info.second);
    }
}

pqxx::connection & Connection::getRef()
{
    connect();
    assert(connection != nullptr);
    return *connection;
}

void Connection::connect()
{
    if (!connection || !connection->is_open())
    {
        /// Always throws if there is no connection.
        connection = std::make_unique<pqxx::connection>(connection_info.first);
        if (replication)
            connection->set_variable("default_transaction_isolation", "'repeatable read'");
        LOG_DEBUG(&Poco::Logger::get("PostgreSQLConnection"), "New connection to {}", connection_info.second);
    }
}
}
