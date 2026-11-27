#include <Core/PostgreSQL/Connection.h>

#if USE_LIBPQXX
#include <Common/logger_useful.h>


namespace postgres
{

Connection::Connection(const ConnectionInfo & connection_info_, bool replication_, size_t num_tries_)
    : connection_info(connection_info_), replication(replication_), num_tries(num_tries_)
    , log(getLogger("PostgreSQLReplicaConnection"))
{
    if (replication)
        connection_info = {fmt::format("{} replication=database", connection_info.connection_string), connection_info.host_port};
}

void Connection::execWithRetry(const std::function<void(pqxx::nontransaction &)> & exec)
{
    for (size_t try_no = 0; try_no < num_tries; ++try_no)
    {
        try
        {
            pqxx::nontransaction tx(getRef());
            exec(tx);
            break;
        }
        catch (const pqxx::broken_connection & e)
        {
            LOG_DEBUG(log, "Cannot execute query due to connection failure, attempt: {}/{}. (Message: {})",
                      try_no, num_tries, e.what());

            if (try_no + 1 == num_tries)
                throw;
        }
    }
}

pqxx::connection & Connection::getRef()
{
    connect();
    return *connection;
}

void Connection::tryUpdateConnection()
{
    try
    {
        updateConnection();
    }
    catch (const pqxx::broken_connection & e)
    {
        LOG_ERROR(log, "Unable to update connection: {}", e.what());
    }
}

void Connection::updateConnection()
{
    if (connection)
        connection->close();

    /// Always throws if there is no connection.
    connection = std::make_unique<pqxx::connection>(connection_info.connection_string);

    if (replication)
        connection->set_variable("default_transaction_isolation", "'repeatable read'");

    LOG_DEBUG(getLogger("PostgreSQLConnection"), "New connection to {}", connection_info.host_port);
}

void Connection::connect()
{
    if (!connection || !connection->is_open())
        updateConnection();
}

}

#endif
