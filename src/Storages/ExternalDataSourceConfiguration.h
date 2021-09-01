#pragma once

#include <Parsers/ASTIdentifier.h>
#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace ExternalDataSource
{
}

namespace DB
{

struct ExternalDataSourceConfiguration
{
    String host;
    UInt16 port;
    String username;
    String password;
    String database;
    String table;

    ExternalDataSourceConfiguration() = default;
    ExternalDataSourceConfiguration(const ExternalDataSourceConfiguration & configuration) = default;
};

struct StoragePostgreSQLConfiguration : ExternalDataSourceConfiguration
{
    explicit StoragePostgreSQLConfiguration(
            const ExternalDataSourceConfiguration & common_configuration = {},
            const String & schema_ = "", const String & on_conflict_ = "")
        : ExternalDataSourceConfiguration(common_configuration)
        , schema(schema_), on_conflict(on_conflict_) {}

    String schema;
    String on_conflict;
    std::vector<std::pair<String, UInt16>> addresses; /// Failover replicas.
};


using EngineArgs = std::vector<std::pair<String, DB::Field>>;

/* If storage engine's configuration was define via named_collections,
 * return all options in ExternalDataSource::Configuration struct.
 *
 * Also check if engine arguemnts have key-value defined configuration options:
 * ENGINE = PostgreSQL(postgresql_configuration, database = 'postgres_database');
 * In this case they will override values defined in config.
 *
 * If there are key-value arguments apart from common: `host`, `port`, `username`, `password`, `database`,
 * i.e. storage-specific arguments, then return them back in a set: ExternalDataSource::EngineArgs.
 */
std::tuple<ExternalDataSourceConfiguration, EngineArgs, bool>
tryGetConfigurationAsNamedCollection(ASTs args, ContextPtr context);

}
