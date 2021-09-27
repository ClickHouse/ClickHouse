#pragma once

#include <Parsers/ASTIdentifier.h>
#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

struct ExternalDataSourceConfiguration
{
    String host;
    UInt16 port = 0;
    String username;
    String password;
    String database;
    String table;
    String schema;

    std::vector<std::pair<String, UInt16>> addresses; /// Failover replicas.

    String toString() const;

    void set(const ExternalDataSourceConfiguration & conf);
};


struct StoragePostgreSQLConfiguration : ExternalDataSourceConfiguration
{
    String on_conflict;
};


struct StorageMySQLConfiguration : ExternalDataSourceConfiguration
{
    bool replace_query = false;
    String on_duplicate_clause;
};

struct StorageMongoDBConfiguration : ExternalDataSourceConfiguration
{
    String collection;
    String options;
};


using StorageSpecificArgs = std::vector<std::pair<String, DB::Field>>;

struct ExternalDataSourceConfig
{
    ExternalDataSourceConfiguration configuration;
    StorageSpecificArgs specific_args;
};

/* If there is a storage engine's configuration specified in the named_collections,
 * this function returns valid for usage ExternalDataSourceConfiguration struct
 * otherwise std::nullopt is returned.
 *
 * If any configuration options are provided as key-value engine arguments, they will override
 * configuration values, i.e. ENGINE = PostgreSQL(postgresql_configuration, database = 'postgres_database');
 *
 * Any key-value engine argument except common (`host`, `port`, `username`, `password`, `database`)
 * is returned in EngineArgs struct.
 */
std::optional<ExternalDataSourceConfig> getExternalDataSourceConfiguration(const ASTs & args, ContextPtr context, bool is_database_engine = false);

std::optional<ExternalDataSourceConfiguration> getExternalDataSourceConfiguration(
    const Poco::Util::AbstractConfiguration & dict_config, const String & dict_config_prefix, ContextPtr context);


/// Highest priority is 0, the bigger the number in map, the less the priority.
using ExternalDataSourcesConfigurationByPriority = std::map<size_t, std::vector<ExternalDataSourceConfiguration>>;

struct ExternalDataSourcesByPriority
{
    String database;
    String table;
    String schema;
    ExternalDataSourcesConfigurationByPriority replicas_configurations;
};

ExternalDataSourcesByPriority
getExternalDataSourceConfigurationByPriority(const Poco::Util::AbstractConfiguration & dict_config, const String & dict_config_prefix, ContextPtr context);


struct URLBasedDataSourceConfiguration
{
    String url;
    String format;
    String compression_method = "auto";
    String structure;

    std::vector<std::pair<String, Field>> headers;

    void set(const URLBasedDataSourceConfiguration & conf);
};

struct StorageS3Configuration : URLBasedDataSourceConfiguration
{
    String access_key_id;
    String secret_access_key;
};

struct URLBasedDataSourceConfig
{
    URLBasedDataSourceConfiguration configuration;
    StorageSpecificArgs specific_args;
};

std::optional<URLBasedDataSourceConfig> getURLBasedDataSourceConfiguration(const ASTs & args, ContextPtr context);

}
