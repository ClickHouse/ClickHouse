#include <Core/PostgreSQL/Connection.h>

#if USE_LIBPQXX
#include <Common/logger_useful.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>


namespace CurrentMetrics
{
    extern const Metric PostgreSQLClientConnections;
    extern const Metric PostgreSQLClientConnectionsIdle;
    extern const Metric PostgreSQLClientConnectionsInUse;
}

namespace ProfileEvents
{
    extern const Event PostgreSQLClientConnectionsCreated;
}

namespace postgres
{

Connection::Lease::Lease(pqxx::connection & connection_) : connection(&connection_)
{
    CurrentMetrics::sub(CurrentMetrics::PostgreSQLClientConnectionsIdle, 1);
    CurrentMetrics::add(CurrentMetrics::PostgreSQLClientConnectionsInUse, 1);
}

Connection::Lease::~Lease()
{
    CurrentMetrics::sub(CurrentMetrics::PostgreSQLClientConnectionsInUse, 1);
    CurrentMetrics::add(CurrentMetrics::PostgreSQLClientConnectionsIdle, 1);
}

Connection::Lease::Lease(const Lease & other) : Lease(*other.connection) {}

Connection::Lease & Connection::Lease::operator=(const Lease & other)
{
    connection = other.connection;
    return *this;
}


Connection::Connection(const ConnectionInfo & connection_info_, bool replication_, size_t num_tries_)
    : connection_info(connection_info_), replication(replication_), num_tries(num_tries_)
    , log(getLogger("PostgreSQLReplicaConnection"))
{
    if (replication)
        connection_info = {fmt::format("{} replication=database", connection_info.connection_string), connection_info.host_port};
}

Connection::~Connection()
{
    close();
}

void Connection::execWithRetry(const std::function<void(pqxx::nontransaction &)> & exec)
{
    for (size_t try_no = 0; try_no < num_tries; ++try_no)
    {
        try
        {
            Lease lease = getLease();
            pqxx::nontransaction tx(lease.getRef());
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

Connection::Lease Connection::getLease()
{
    connect();
    return Lease(*connection);
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
    close();

    /// Always throws if there is no connection.
    connection = std::make_unique<pqxx::connection>(connection_info.connection_string);

    CurrentMetrics::add(CurrentMetrics::PostgreSQLClientConnections, 1);
    CurrentMetrics::add(CurrentMetrics::PostgreSQLClientConnectionsIdle, 1);
    ProfileEvents::increment(ProfileEvents::PostgreSQLClientConnectionsCreated);

    if (replication)
        connection->set_variable("default_transaction_isolation", "'repeatable read'");

    LOG_DEBUG(getLogger("PostgreSQLConnection"), "New connection to {}", connection_info.host_port);
}

void Connection::connect()
{
    if (!connection || !connection->is_open())
        updateConnection();
}

void Connection::close()
{
    if (connection) {
        CurrentMetrics::sub(CurrentMetrics::PostgreSQLClientConnectionsIdle, 1);
        CurrentMetrics::sub(CurrentMetrics::PostgreSQLClientConnections, 1);

        connection.reset();
    }
}

}

#endif
