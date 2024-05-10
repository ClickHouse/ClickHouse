#include "ExternalDataSourceConfiguration.h"

#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <IO/WriteBufferFromString.h>

#include <re2/re2.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

IMPLEMENT_SETTINGS_TRAITS(EmptySettingsTraits, EMPTY_SETTINGS)

static const std::unordered_set<std::string_view> dictionary_allowed_keys = {
    "host", "port", "user", "password", "quota_key", "db",
    "database", "table", "schema", "replica",
    "update_field", "update_lag", "invalidate_query", "query",
    "where", "name", "secure", "uri", "collection"};


template<typename T>
SettingsChanges getSettingsChangesFromConfig(
    const BaseSettings<T> & settings, const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    SettingsChanges config_settings;
    for (const auto & setting : settings.all())
    {
        const auto & setting_name = setting.getName();
        auto setting_value = config.getString(config_prefix + '.' + setting_name, "");
        if (!setting_value.empty())
            config_settings.emplace_back(setting_name, setting_value);
    }
    return config_settings;
}


String ExternalDataSourceConfiguration::toString() const
{
    WriteBufferFromOwnString configuration_info;
    configuration_info << "username: " << username << "\t";
    if (addresses.empty())
    {
        configuration_info << "host: " << host << "\t";
        configuration_info << "port: " << port << "\t";
    }
    else
    {
        for (const auto & [replica_host, replica_port] : addresses)
        {
            configuration_info << "host: " << replica_host << "\t";
            configuration_info << "port: " << replica_port << "\t";
        }
    }
    return configuration_info.str();
}


void ExternalDataSourceConfiguration::set(const ExternalDataSourceConfiguration & conf)
{
    host = conf.host;
    port = conf.port;
    username = conf.username;
    password = conf.password;
    quota_key = conf.quota_key;
    database = conf.database;
    table = conf.table;
    schema = conf.schema;
    addresses = conf.addresses;
    addresses_expr = conf.addresses_expr;
}


static void validateConfigKeys(
    const Poco::Util::AbstractConfiguration & dict_config, const String & config_prefix, HasConfigKeyFunc has_config_key_func)
{
    Poco::Util::AbstractConfiguration::Keys config_keys;
    dict_config.keys(config_prefix, config_keys);
    for (const auto & config_key : config_keys)
    {
        if (!has_config_key_func(config_key))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected key `{}` in dictionary source configuration", config_key);
    }
}

template <typename T>
std::optional<ExternalDataSourceInfo> getExternalDataSourceConfiguration(
    const Poco::Util::AbstractConfiguration & dict_config, const String & dict_config_prefix,
    ContextPtr context, HasConfigKeyFunc has_config_key, const BaseSettings<T> & settings)
{
    validateConfigKeys(dict_config, dict_config_prefix, has_config_key);
    ExternalDataSourceConfiguration configuration;

    auto collection_name = dict_config.getString(dict_config_prefix + ".name", "");
    if (!collection_name.empty())
    {
        const auto & config = context->getConfigRef();
        const auto & collection_prefix = fmt::format("named_collections.{}", collection_name);
        validateConfigKeys(dict_config, collection_prefix, has_config_key);
        auto config_settings = getSettingsChangesFromConfig(settings, config, collection_prefix);
        auto dict_settings = getSettingsChangesFromConfig(settings, dict_config, dict_config_prefix);
        /// dictionary config settings override collection settings.
        config_settings.insert(config_settings.end(), dict_settings.begin(), dict_settings.end());

        if (!config.has(collection_prefix))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no collection named `{}` in config", collection_name);

        configuration.host = dict_config.getString(dict_config_prefix + ".host", config.getString(collection_prefix + ".host", ""));
        configuration.port = dict_config.getInt(dict_config_prefix + ".port", config.getUInt(collection_prefix + ".port", 0));
        configuration.username = dict_config.getString(dict_config_prefix + ".user", config.getString(collection_prefix + ".user", ""));
        configuration.password = dict_config.getString(dict_config_prefix + ".password", config.getString(collection_prefix + ".password", ""));
        configuration.quota_key = dict_config.getString(dict_config_prefix + ".quota_key", config.getString(collection_prefix + ".quota_key", ""));
        configuration.database = dict_config.getString(dict_config_prefix + ".db", config.getString(dict_config_prefix + ".database",
            config.getString(collection_prefix + ".db", config.getString(collection_prefix + ".database", ""))));
        configuration.table = dict_config.getString(dict_config_prefix + ".table", config.getString(collection_prefix + ".table", ""));
        configuration.schema = dict_config.getString(dict_config_prefix + ".schema", config.getString(collection_prefix + ".schema", ""));

        if (configuration.host.empty() || configuration.port == 0 || configuration.username.empty() || configuration.table.empty())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Named collection of connection parameters is missing some "
                            "of the parameters and dictionary parameters are not added");
        }
        return ExternalDataSourceInfo{.configuration = configuration, .settings_changes = config_settings};
    }
    return std::nullopt;
}

std::optional<URLBasedDataSourceConfig> getURLBasedDataSourceConfiguration(
    const Poco::Util::AbstractConfiguration & dict_config, const String & dict_config_prefix, ContextPtr context)
{
    URLBasedDataSourceConfiguration configuration;
    auto collection_name = dict_config.getString(dict_config_prefix + ".name", "");
    if (!collection_name.empty())
    {
        const auto & config = context->getConfigRef();
        const auto & collection_prefix = fmt::format("named_collections.{}", collection_name);

        if (!config.has(collection_prefix))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no collection named `{}` in config", collection_name);

        configuration.url =
            dict_config.getString(dict_config_prefix + ".url", config.getString(collection_prefix + ".url", ""));
        configuration.endpoint =
            dict_config.getString(dict_config_prefix + ".endpoint", config.getString(collection_prefix + ".endpoint", ""));
        configuration.format =
            dict_config.getString(dict_config_prefix + ".format", config.getString(collection_prefix + ".format", ""));
        configuration.compression_method =
            dict_config.getString(dict_config_prefix + ".compression", config.getString(collection_prefix + ".compression_method", ""));
        configuration.structure =
            dict_config.getString(dict_config_prefix + ".structure", config.getString(collection_prefix + ".structure", ""));
        configuration.user =
            dict_config.getString(dict_config_prefix + ".credentials.user", config.getString(collection_prefix + ".credentials.user", ""));
        configuration.password =
            dict_config.getString(dict_config_prefix + ".credentials.password", config.getString(collection_prefix + ".credentials.password", ""));

        String headers_prefix;
        const Poco::Util::AbstractConfiguration *headers_config = nullptr;
        if (dict_config.has(dict_config_prefix + ".headers"))
        {
            headers_prefix = dict_config_prefix + ".headers";
            headers_config = &dict_config;
        }
        else
        {
            headers_prefix = collection_prefix + ".headers";
            headers_config = &config;
        }

        if (headers_config)
        {
            Poco::Util::AbstractConfiguration::Keys header_keys;
            headers_config->keys(headers_prefix, header_keys);
            headers_prefix += ".";
            for (const auto & header : header_keys)
            {
                const auto header_prefix = headers_prefix + header;
                configuration.headers.emplace_back(
                    headers_config->getString(header_prefix + ".name"),
                    headers_config->getString(header_prefix + ".value"));
            }
        }

        return URLBasedDataSourceConfig{ .configuration = configuration };
    }

    return std::nullopt;
}

ExternalDataSourcesByPriority getExternalDataSourceConfigurationByPriority(
    const Poco::Util::AbstractConfiguration & dict_config, const String & dict_config_prefix, ContextPtr context, HasConfigKeyFunc has_config_key)
{
    validateConfigKeys(dict_config, dict_config_prefix, has_config_key);
    ExternalDataSourceConfiguration common_configuration;

    auto named_collection = getExternalDataSourceConfiguration(dict_config, dict_config_prefix, context, has_config_key);
    if (named_collection)
    {
        common_configuration = named_collection->configuration;
    }
    else
    {
        common_configuration.host = dict_config.getString(dict_config_prefix + ".host", "");
        common_configuration.port = dict_config.getUInt(dict_config_prefix + ".port", 0);
        common_configuration.username = dict_config.getString(dict_config_prefix + ".user", "");
        common_configuration.password = dict_config.getString(dict_config_prefix + ".password", "");
        common_configuration.quota_key = dict_config.getString(dict_config_prefix + ".quota_key", "");
        common_configuration.database = dict_config.getString(dict_config_prefix + ".db", dict_config.getString(dict_config_prefix + ".database", ""));
        common_configuration.table = dict_config.getString(fmt::format("{}.table", dict_config_prefix), "");
        common_configuration.schema = dict_config.getString(fmt::format("{}.schema", dict_config_prefix), "");
    }

    ExternalDataSourcesByPriority configuration
    {
        .database = common_configuration.database,
        .table = common_configuration.table,
        .schema = common_configuration.schema,
        .replicas_configurations = {}
    };

    if (dict_config.has(dict_config_prefix + ".replica"))
    {
        Poco::Util::AbstractConfiguration::Keys config_keys;
        dict_config.keys(dict_config_prefix, config_keys);

        for (const auto & config_key : config_keys)
        {
            if (config_key.starts_with("replica"))
            {
                ExternalDataSourceConfiguration replica_configuration(common_configuration);
                String replica_name = dict_config_prefix + "." + config_key;
                validateConfigKeys(dict_config, replica_name, has_config_key);

                size_t priority = dict_config.getInt(replica_name + ".priority", 0);
                replica_configuration.host = dict_config.getString(replica_name + ".host", common_configuration.host);
                replica_configuration.port = dict_config.getUInt(replica_name + ".port", common_configuration.port);
                replica_configuration.username = dict_config.getString(replica_name + ".user", common_configuration.username);
                replica_configuration.password = dict_config.getString(replica_name + ".password", common_configuration.password);
                replica_configuration.quota_key = dict_config.getString(replica_name + ".quota_key", common_configuration.quota_key);

                if (replica_configuration.host.empty() || replica_configuration.port == 0
                    || replica_configuration.username.empty() || replica_configuration.password.empty())
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "Named collection of connection parameters is missing some "
                                    "of the parameters and no other dictionary parameters are added");
                }

                configuration.replicas_configurations[priority].emplace_back(replica_configuration);
            }
        }
    }
    else
    {
        configuration.replicas_configurations[0].emplace_back(common_configuration);
    }

    return configuration;
}


void URLBasedDataSourceConfiguration::set(const URLBasedDataSourceConfiguration & conf)
{
    url = conf.url;
    format = conf.format;
    compression_method = conf.compression_method;
    structure = conf.structure;
    http_method = conf.http_method;
    headers = conf.headers;
}

template
std::optional<ExternalDataSourceInfo> getExternalDataSourceConfiguration(
    const Poco::Util::AbstractConfiguration & dict_config, const String & dict_config_prefix,
    ContextPtr context, HasConfigKeyFunc has_config_key, const BaseSettings<EmptySettingsTraits> & settings);

template
SettingsChanges getSettingsChangesFromConfig(
    const BaseSettings<EmptySettingsTraits> & settings, const Poco::Util::AbstractConfiguration & config, const String & config_prefix);

}
