#pragma once

#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Storages/StorageS3Settings.h>


namespace DB
{

#define EMPTY_SETTINGS(M)
DECLARE_SETTINGS_TRAITS(EmptySettingsTraits, EMPTY_SETTINGS)

struct EmptySettings : public BaseSettings<EmptySettingsTraits> {};

struct ExternalDataSourceConfiguration
{
    String host;
    UInt16 port = 0;
    String username = "default";
    String password;
    String database;
    String table;
    String schema;

    std::vector<std::pair<String, UInt16>> addresses; /// Failover replicas.
    String addresses_expr;

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
    String options;
};


using StorageSpecificArgs = std::vector<std::pair<String, ASTPtr>>;

struct ExternalDataSourceInfo
{
    ExternalDataSourceConfiguration configuration;
    StorageSpecificArgs specific_args;
    SettingsChanges settings_changes;
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
template <typename T = EmptySettingsTraits>
std::optional<ExternalDataSourceInfo> getExternalDataSourceConfiguration(
    const ASTs & args, ContextPtr context, bool is_database_engine = false, bool throw_on_no_collection = true, const BaseSettings<T> & storage_settings = {});

using HasConfigKeyFunc = std::function<bool(const String &)>;

template <typename T = EmptySettingsTraits>
std::optional<ExternalDataSourceInfo> getExternalDataSourceConfiguration(
    const Poco::Util::AbstractConfiguration & dict_config, const String & dict_config_prefix,
    ContextPtr context, HasConfigKeyFunc has_config_key, const BaseSettings<T> & settings = {});


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
getExternalDataSourceConfigurationByPriority(const Poco::Util::AbstractConfiguration & dict_config, const String & dict_config_prefix, ContextPtr context, HasConfigKeyFunc has_config_key);


struct URLBasedDataSourceConfiguration
{
    String url;
    String endpoint;
    String format = "auto";
    String compression_method = "auto";
    String structure = "auto";

    String user;
    String password;

    std::vector<std::pair<String, Field>> headers;
    String http_method;

    void set(const URLBasedDataSourceConfiguration & conf);
};

struct StorageS3Configuration : URLBasedDataSourceConfiguration
{
    S3Settings::AuthSettings auth_settings;
    S3Settings::ReadWriteSettings rw_settings;
};


struct StorageS3ClusterConfiguration : StorageS3Configuration
{
    String cluster_name;
};

struct URLBasedDataSourceConfig
{
    URLBasedDataSourceConfiguration configuration;
    StorageSpecificArgs specific_args;
};

std::optional<URLBasedDataSourceConfig> getURLBasedDataSourceConfiguration(const ASTs & args, ContextPtr context);

std::optional<URLBasedDataSourceConfig> getURLBasedDataSourceConfiguration(
    const Poco::Util::AbstractConfiguration & dict_config, const String & dict_config_prefix, ContextPtr context);

template<typename T>
bool getExternalDataSourceConfiguration(const ASTs & args, BaseSettings<T> & settings, ContextPtr context);

}
