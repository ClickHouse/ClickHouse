#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL

#include <Databases/MySQL/DatabaseWithMySQLConnection.h>
#include <Common/quoteString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_DATABASE_STATUS;
}

DatabaseWithMySQLConnection::DatabaseWithMySQLConnection(const String & database_name_, const MySQLConnectionArgs & args)
    : IDatabase(database_name_), connection_args(args)
{
}

void DatabaseWithMySQLConnection::loadStoredObjects(Context & /*context*/, bool /*has_force_restore_data_flag*/, bool /*force_attach*/)
{
    pool.emplace(connection_args.database_name, connection_args.hostname,
        connection_args.username, connection_args.user_password, connection_args.port);

    client.emplace(connection_args.hostname, connection_args.port, connection_args.username, connection_args.user_password);
}

MySQLClient & DatabaseWithMySQLConnection::getMySQLReplicaClient() const
{
    /// TODO: test mysql server status.
    return *client;
}

mysqlxx::Pool::Entry DatabaseWithMySQLConnection::getMySQLConnection() const
{
    try
    {
        if (!pool.has_value())  /// It seems impossible
            throw Exception("LOGICAL_ERROR: it is a bug.", ErrorCodes::LOGICAL_ERROR);

        return pool->get();
    }
    catch (...)
    {
        const auto & exception_message = getCurrentExceptionMessage(true);
        throw Exception(
            "MySQL database server is unavailable(hostname:" + backQuoteIfNeed(connection_args.hostname) +
            ", port:" + toString(connection_args.port) + ", database:" + backQuoteIfNeed(connection_args.database_name) + ", user:" +
            backQuoteIfNeed(connection_args.username) + "), exception message:" + exception_message + ".", ErrorCodes::ILLEGAL_DATABASE_STATUS);
    }
}

mysqlxx::Pool DatabaseWithMySQLConnection::createNewMySQLConnectionsPool() const
{
    mysqlxx::Pool new_connections_pool(connection_args.database_name, connection_args.hostname,
        connection_args.username, connection_args.user_password, connection_args.port);
    return new_connections_pool;
}
}

#endif
