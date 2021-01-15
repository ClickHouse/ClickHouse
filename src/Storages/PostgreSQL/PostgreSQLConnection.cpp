#if !defined(ARCADIA_BUILD)
#include "config_core.h"
#endif

#if USE_LIBPQXX
#include <Storages/PostgreSQL/PostgreSQLConnection.h>


namespace DB
{

PostgreSQLConnection::ConnectionPtr conn()
{
    checkUpdateConnection();
    return connection;
}

void PostgreSQLConnection::checkUpdateConnection()
{
    if (!connection || !connection->is_open())
        connection = std::make_unique<pqxx::connection>(connection_str);
}

}

#endif
