#pragma once

#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Storages/StorageS3Settings.h>
#include <IO/HTTPHeaderEntries.h>


namespace DB
{

#define EMPTY_SETTINGS(M, ALIAS)
DECLARE_SETTINGS_TRAITS(EmptySettingsTraits, EMPTY_SETTINGS)

struct EmptySettings : public BaseSettings<EmptySettingsTraits> {};

struct ExternalDataSourceConfiguration
{
    String host;
    UInt16 port = 0;
    String username = "default";
    String password;
    String quota_key;
    String database;
    String table;
    String schema;

    std::vector<std::pair<String, UInt16>> addresses; /// Failover replicas.
    String addresses_expr;

    String toString() const;

    void set(const ExternalDataSourceConfiguration & conf);
};


using StorageSpecificArgs = std::vector<std::pair<String, ASTPtr>>;

struct ExternalDataSourceInfo
{
    ExternalDataSourceConfiguration configuration;
    SettingsChanges settings_changes;
};

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

    HTTPHeaderEntries headers;
    String http_method;

    void set(const URLBasedDataSourceConfiguration & conf);
};

struct URLBasedDataSourceConfig
{
    URLBasedDataSourceConfiguration configuration;
};

std::optional<URLBasedDataSourceConfig> getURLBasedDataSourceConfiguration(
    const Poco::Util::AbstractConfiguration & dict_config, const String & dict_config_prefix, ContextPtr context);

}
