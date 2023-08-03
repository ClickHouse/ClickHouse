#pragma once

#include <Core/Defines.h>
#include <Core/BaseSettings.h>
#include <Core/SettingsEnums.h>
#include <Interpreters/Context_fwd.h>


namespace Poco::Util
{
    class AbstractConfiguration;
}


namespace DB
{
class ASTStorage;
class ASTSetQuery;

#define LIST_OF_POSTGRESQL_SETTINGS(M, ALIAS) \
    M(UInt64, connection_pool_size, 16, "Size of connection pool (if all connections are in use, the query will wait until some connection will be freed).", 0) \
    M(UInt64, connection_max_tries, 3, "Number of retries for pool with failover", 0) \
    M(UInt64, connection_pool_wait_timeout, 5, "Timeout (in seconds) for waiting for free connection (in case of there is already connection_pool_size active connections), 0 - do not wait.", 0) \
    M(Bool, connection_pool_auto_close, true, "Auto-close connection after query execution, i.e. disable connection reuse.", 0)

DECLARE_SETTINGS_TRAITS(PostgreSQLSettingsTraits, LIST_OF_POSTGRESQL_SETTINGS)


using PostgreSQLBaseSettings = BaseSettings<PostgreSQLSettingsTraits>;

/** Settings for the PostgreSQL family of engines.
  */
struct PostgreSQLSettings : public PostgreSQLBaseSettings
{
    void loadFromQuery(ASTStorage & storage_def);
    void loadFromQuery(const ASTSetQuery & settings_def);
};


}
