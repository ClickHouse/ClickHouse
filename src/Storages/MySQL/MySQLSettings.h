#pragma once

#include <Core/Defines.h>
#include <Core/BaseSettings.h>


namespace Poco::Util
{
    class AbstractConfiguration;
}


namespace DB
{
class ASTStorage;

#define LIST_OF_MYSQL_SETTINGS(M) \
    M(UInt64, connection_pool_size, 16, "Size of connection pool (if all connections are in use, the query will wait until some connection will be freed).", 0) \
    M(UInt64, connection_max_tries, 3, "Number of retries for pool with failover", 0) \
    M(Bool, connection_auto_close, true, "Auto-close connection after query execution, i.e. disable connection reuse.", 0) \

DECLARE_SETTINGS_TRAITS(MySQLSettingsTraits, LIST_OF_MYSQL_SETTINGS)


/** Settings for the MySQL family of engines.
  */
struct MySQLSettings : public BaseSettings<MySQLSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);
};

}
