#include <Databases/MySQL/DatabaseWithMySQLConnection.h>

namespace DB
{

DatabaseWithMySQLConnection::DatabaseWithMySQLConnection(const String & database_name_, const MySQLConnectionArgs & args)
    : IDatabase(database_name_), connection_args(args)
{
}

void DatabaseWithMySQLConnection::loadStoredObjects(Context & /*context*/, bool /*has_force_restore_data_flag*/, bool /*force_attach*/)
{
    try
    {
        pool.emplace(connection_args.database_name, connection_args.hostname,
            connection_args.username, connection_args.user_password, connection_args.port);

        client.emplace(connection_args.hostname, connection_args.port, connection_args.username, connection_args.user_password);
    }
    catch (...)
    {
        /// TODO: 在此报错, 尝试链接MySQL. 如果是force_attach则延后报错.
    }
//    IDatabase::loadStoredObjects(context, b, b1);
}

MySQLClient & DatabaseWithMySQLConnection::getMySQLReplicaClient() const
{
    return *client;
}

mysqlxx::Pool & DatabaseWithMySQLConnection::getMySQLConnectionPool() const
{
    return *pool;
}
}
