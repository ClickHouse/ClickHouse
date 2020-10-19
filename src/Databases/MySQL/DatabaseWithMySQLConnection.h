#pragma once

//#if USE_MYSQL

#    include <Databases/IDatabase.h>

#    include <optional>
#    include <mysqlxx/Pool.h>
#    include <Core/MySQL/MySQLClient.h>

namespace DB
{

struct MySQLConnectionArgs
{
    String hostname;
    UInt16 port;
    String database_name;
    String username;
    String user_password;
};

class DatabaseWithMySQLConnection : public IDatabase
{
public:
    DatabaseWithMySQLConnection(const String & database_name, const MySQLConnectionArgs & args);

    void loadStoredObjects(Context & context, bool has_force_restore_data_flag, bool force_attach) override;

    MySQLClient & getMySQLReplicaClient() const;

    mysqlxx::Pool & getMySQLConnectionPool() const;

private:
    MySQLConnectionArgs connection_args;

    mutable std::optional<mysqlxx::Pool> pool;
    mutable std::optional<MySQLClient> client;
};

}

//#endif
