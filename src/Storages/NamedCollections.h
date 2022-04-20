#pragma once

#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

/**
 * Given config_ref and list of ASTs, check if this list refers to a named collection.
 * E.g. first argument is an AST Identifier with a name of some named collection from config.
 */
bool isNamedCollection(const ASTs & args, const Poco::Util::AbstractConfiguration & config);

/**
 * Same, but checks not in AST, but by NAME key in config. (Used for dictionary sourses).
 */
bool isNamedCollection(const Poco::Util::AbstractConfiguration & config, const String & config_prefix);

String getCollectionName(const ASTs & args);
String getCollectionName(const Poco::Util::AbstractConfiguration & config, const String & config_prefix);


struct ConfigKeyInfo
{
    Field::Types::Which type;
    std::optional<Field> default_value;
};

using NamedConfiguration = std::unordered_map<String, ConfigKeyInfo>;
using ConfigurationFromNamedCollection = std::unordered_map<String, Field>;
using ConfigurationsFromNamedCollection = std::vector<ConfigurationFromNamedCollection>;

/**
 * Get configuration from config by collection name.
 * Configuration is listed like:
 * <named_collections>
 *     <collection_name>
 *     </collection_name>
 * </named_collections>
 */
ConfigurationFromNamedCollection getConfigurationFromNamedCollection(
    const String & collection_name,
    const Poco::Util::AbstractConfiguration & config,
    const std::unordered_map<String, ConfigKeyInfo> & keys);

/**
 * Configuration can be defined as (collection_name, key1=value1, key2=value2, ...).
 * In this case key-value arguments override config values from named collection.
 */
void overrideConfigurationFromNamedCollectionWithAST(
    ASTs & args,
    ConfigurationFromNamedCollection & configuration,
    const std::unordered_map<String, ConfigKeyInfo> & keys,
    ContextPtr context);

/**
 * Get configuration represented as root configuration and some listed configuration.
 * Root configuration can be common to all listed configurations, listed configuration
 * overrides common configuration. Listed configuration is defined by enumerate_by_key prefix.
 */
struct ListedConfigurationFromNamedCollection
{
    ConfigurationFromNamedCollection root_configuration;
    ConfigurationsFromNamedCollection listed_configurations;
};

ListedConfigurationFromNamedCollection getListedConfigurationFromNamedCollection(
    const String & collection_name,
    const Poco::Util::AbstractConfiguration & config,
    const std::unordered_map<String, ConfigKeyInfo> & keys,
    const String & enumerate_by_key);


String toString(const ConfigurationFromNamedCollection & configuration);

/**
 * Given a list of keys and config prefix, get a list of key-value pairs for each key in `keys`.
 */
ConfigurationFromNamedCollection parseConfigKeys(
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    const std::unordered_map<String, ConfigKeyInfo> & keys);

void validateConfigKeys(
    const Poco::Util::AbstractConfiguration & dict_config,
    const String & config_prefix,
    const std::unordered_map<String, ConfigKeyInfo> & keys,
    const String & prefix);


/// Base for all storage configurations. Contains common fields.
struct StorageConfiguration
{
    String format = "auto";
    String compression_method = "auto";
    String structure = "auto";
};

}
