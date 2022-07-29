#include "ExternalDataSourceConfiguration.h"

#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <IO/WriteBufferFromString.h>

#if USE_AMQPCPP
#include <Storages/RabbitMQ/RabbitMQSettings.h>
#endif
#if USE_RDKAFKA
#include <Storages/Kafka/KafkaSettings.h>
#endif
#if USE_MYSQL
#include <Storages/MySQL/MySQLSettings.h>
#endif
#if USE_NATSIO
#include <Storages/NATS/NATSSettings.h>
#endif

#include <re2/re2.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

IMPLEMENT_SETTINGS_TRAITS(EmptySettingsTraits, EMPTY_SETTINGS)

static const std::unordered_set<std::string_view> dictionary_allowed_keys = {
    "host", "port", "user", "password", "db",
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
    database = conf.database;
    table = conf.table;
    schema = conf.schema;
    addresses = conf.addresses;
    addresses_expr = conf.addresses_expr;
}


template <typename T>
std::optional<ExternalDataSourceInfo> getExternalDataSourceConfiguration(
    const ASTs & args, ContextPtr context, bool is_database_engine, bool throw_on_no_collection, const BaseSettings<T> & storage_settings)
{
    if (args.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "External data source must have arguments");

    ExternalDataSourceConfiguration configuration;
    StorageSpecificArgs non_common_args;

    if (const auto * collection = typeid_cast<const ASTIdentifier *>(args[0].get()))
    {
        const auto & config = context->getConfigRef();
        const auto & collection_prefix = fmt::format("named_collections.{}", collection->name());

        if (!config.has(collection_prefix))
        {
            /// For table function remote we do not throw on no collection, because then we consider first arg
            /// as cluster definition from config.
            if (!throw_on_no_collection)
                return std::nullopt;

            throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no collection named `{}` in config", collection->name());
        }

        SettingsChanges config_settings = getSettingsChangesFromConfig(storage_settings, config, collection_prefix);

        configuration.host = config.getString(collection_prefix + ".host", "");
        configuration.port = config.getInt(collection_prefix + ".port", 0);
        configuration.username = config.getString(collection_prefix + ".user", "");
        configuration.password = config.getString(collection_prefix + ".password", "");
        configuration.database = config.getString(collection_prefix + ".database", "");
        configuration.table = config.getString(collection_prefix + ".table", config.getString(collection_prefix + ".collection", ""));
        configuration.schema = config.getString(collection_prefix + ".schema", "");
        configuration.addresses_expr = config.getString(collection_prefix + ".addresses_expr", "");

        if (!configuration.addresses_expr.empty() && !configuration.host.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot have `addresses_expr` and `host`, `port` in configuration at the same time");

        if ((args.size() == 1) && ((configuration.addresses_expr.empty() && (configuration.host.empty() || configuration.port == 0))
            || configuration.database.empty() || (configuration.table.empty() && !is_database_engine)))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Named collection of connection parameters is missing some of the parameters and no key-value arguments are added");
        }

        /// Check key-value arguments.
        for (size_t i = 1; i < args.size(); ++i)
        {
            if (const auto * ast_function = typeid_cast<const ASTFunction *>(args[i].get()))
            {
                const auto * args_expr = assert_cast<const ASTExpressionList *>(ast_function->arguments.get());
                auto function_args = args_expr->children;
                if (function_args.size() != 2)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected key-value defined argument");

                auto arg_name = function_args[0]->as<ASTIdentifier>()->name();
                if (function_args[1]->as<ASTFunction>())
                {
                    non_common_args.emplace_back(std::make_pair(arg_name, function_args[1]));
                    continue;
                }

                auto arg_value_ast = evaluateConstantExpressionOrIdentifierAsLiteral(function_args[1], context);
                auto * arg_value_literal = arg_value_ast->as<ASTLiteral>();
                if (arg_value_literal)
                {
                    auto arg_value = arg_value_literal->value;

                    if (arg_name == "host")
                        configuration.host = arg_value.safeGet<String>();
                    else if (arg_name == "port")
                        configuration.port = arg_value.safeGet<UInt64>();
                    else if (arg_name == "user")
                        configuration.username = arg_value.safeGet<String>();
                    else if (arg_name == "password")
                        configuration.password = arg_value.safeGet<String>();
                    else if (arg_name == "database")
                        configuration.database = arg_value.safeGet<String>();
                    else if (arg_name == "table")
                        configuration.table = arg_value.safeGet<String>();
                    else if (arg_name == "schema")
                        configuration.schema = arg_value.safeGet<String>();
                    else if (arg_name == "addresses_expr")
                        configuration.addresses_expr = arg_value.safeGet<String>();
                    else if (storage_settings.has(arg_name))
                        config_settings.emplace_back(arg_name, arg_value);
                    else
                        non_common_args.emplace_back(std::make_pair(arg_name, arg_value_ast));
                }
                else
                {
                    non_common_args.emplace_back(std::make_pair(arg_name, arg_value_ast));
                }
            }
            else
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected key-value defined argument");
            }
        }

        return ExternalDataSourceInfo{ .configuration = configuration, .specific_args = non_common_args, .settings_changes = config_settings };
    }
    return std::nullopt;
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
        configuration.database = dict_config.getString(dict_config_prefix + ".db", config.getString(dict_config_prefix + ".database",
            config.getString(collection_prefix + ".db", config.getString(collection_prefix + ".database", ""))));
        configuration.table = dict_config.getString(dict_config_prefix + ".table", config.getString(collection_prefix + ".table", ""));
        configuration.schema = dict_config.getString(dict_config_prefix + ".schema", config.getString(collection_prefix + ".schema", ""));

        if (configuration.host.empty() || configuration.port == 0 || configuration.username.empty() || configuration.table.empty())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Named collection of connection parameters is missing some of the parameters and dictionary parameters are not added");
        }
        return ExternalDataSourceInfo{ .configuration = configuration, .specific_args = {}, .settings_changes = config_settings };
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
                    std::make_pair(headers_config->getString(header_prefix + ".name"), headers_config->getString(header_prefix + ".value")));
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

                if (replica_configuration.host.empty() || replica_configuration.port == 0
                    || replica_configuration.username.empty() || replica_configuration.password.empty())
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "Named collection of connection parameters is missing some of the parameters and no other dictionary parameters are added");
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


std::optional<URLBasedDataSourceConfig> getURLBasedDataSourceConfiguration(const ASTs & args, ContextPtr context)
{
    if (args.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "External data source must have arguments");

    URLBasedDataSourceConfiguration configuration;
    StorageSpecificArgs non_common_args;

    if (const auto * collection = typeid_cast<const ASTIdentifier *>(args[0].get()))
    {
        const auto & config = context->getConfigRef();
        auto config_prefix = fmt::format("named_collections.{}", collection->name());

        if (!config.has(config_prefix))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no collection named `{}` in config", collection->name());

        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys(config_prefix, keys);
        for (const auto & key : keys)
        {
            if (key == "url")
            {
                configuration.url = config.getString(config_prefix + ".url", "");
            }
            else if (key == "method")
            {
                configuration.http_method = config.getString(config_prefix + ".method", "");
            }
            else if (key == "format")
            {
                configuration.format = config.getString(config_prefix + ".format", "");
            }
            else if (key == "structure")
            {
                configuration.structure = config.getString(config_prefix + ".structure", "");
            }
            else if (key == "compression_method")
            {
                configuration.compression_method = config.getString(config_prefix + ".compression_method", "");
            }
            else if (key == "headers")
            {
                Poco::Util::AbstractConfiguration::Keys header_keys;
                config.keys(config_prefix + ".headers", header_keys);
                for (const auto & header : header_keys)
                {
                    const auto header_prefix = config_prefix + ".headers." + header;
                    configuration.headers.emplace_back(std::make_pair(config.getString(header_prefix + ".name"), config.getString(header_prefix + ".value")));
                }
            }
            else
            {
                auto value = config.getString(config_prefix + '.' + key);
                non_common_args.emplace_back(std::make_pair(key, std::make_shared<ASTLiteral>(value)));
            }
        }

        /// Check key-value arguments.
        for (size_t i = 1; i < args.size(); ++i)
        {
            if (const auto * ast_function = typeid_cast<const ASTFunction *>(args[i].get()))
            {
                const auto * args_expr = assert_cast<const ASTExpressionList *>(ast_function->arguments.get());
                auto function_args = args_expr->children;
                if (function_args.size() != 2)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected key-value defined argument");

                auto arg_name = function_args[0]->as<ASTIdentifier>()->name();
                auto arg_value_ast = evaluateConstantExpressionOrIdentifierAsLiteral(function_args[1], context);
                auto arg_value = arg_value_ast->as<ASTLiteral>()->value;

                if (arg_name == "url")
                    configuration.url = arg_value.safeGet<String>();
                else if (arg_name == "method")
                    configuration.http_method = arg_value.safeGet<String>();
                else if (arg_name == "format")
                    configuration.format = arg_value.safeGet<String>();
                else if (arg_name == "compression_method")
                    configuration.compression_method = arg_value.safeGet<String>();
                else if (arg_name == "structure")
                    configuration.structure = arg_value.safeGet<String>();
                else
                    non_common_args.emplace_back(std::make_pair(arg_name, arg_value_ast));
            }
            else
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected key-value defined argument");
            }
        }

        if (configuration.url.empty() || configuration.format.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Storage requires {}", configuration.url.empty() ? "url" : "format");

        URLBasedDataSourceConfig source_config{ .configuration = configuration, .specific_args = non_common_args };
        return source_config;
    }
    return std::nullopt;
}


template<typename T>
bool getExternalDataSourceConfiguration(const ASTList & args, BaseSettings<T> & settings, ContextPtr context)
{
    if (args.empty())
        return false;

    if (const auto * collection = typeid_cast<const ASTIdentifier *>(args.front().get()))
    {
        const auto & config = context->getConfigRef();
        const auto & config_prefix = fmt::format("named_collections.{}", collection->name());

        if (!config.has(config_prefix))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no collection named `{}` in config", collection->name());

        auto config_settings = getSettingsChangesFromConfig(settings, config, config_prefix);

        /// Check key-value arguments.
        for (auto it = args.begin(); it != args.end(); ++it)
        {
            if (it == args.begin())
                continue;

            if (const auto * ast_function = typeid_cast<const ASTFunction *>(it->get()))
            {
                const auto * args_expr = assert_cast<const ASTExpressionList *>(ast_function->arguments.get());
                auto function_args = args_expr->children;
                if (function_args.size() != 2)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected key-value defined argument");

                auto arg_name = function_args[0]->as<ASTIdentifier>()->name();
                auto arg_value_ast = evaluateConstantExpressionOrIdentifierAsLiteral(function_args[1], context);
                auto arg_value = arg_value_ast->as<ASTLiteral>()->value;
                config_settings.emplace_back(arg_name, arg_value);
            }
            else
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected key-value defined argument");
            }
        }

        settings.applyChanges(config_settings);
        return true;
    }
    return false;
}

#if USE_AMQPCPP
template
bool getExternalDataSourceConfiguration(const ASTList & args, BaseSettings<RabbitMQSettingsTraits> & settings, ContextPtr context);
#endif

#if USE_RDKAFKA
template
bool getExternalDataSourceConfiguration(const ASTList & args, BaseSettings<KafkaSettingsTraits> & settings, ContextPtr context);
#endif

#if USE_NATSIO
template
bool getExternalDataSourceConfiguration(const ASTList & args, BaseSettings<NATSSettingsTraits> & settings, ContextPtr context);
#endif

template
std::optional<ExternalDataSourceInfo> getExternalDataSourceConfiguration(
    const ASTList & args, ContextPtr context, bool is_database_engine, bool throw_on_no_collection, const BaseSettings<EmptySettingsTraits> & storage_settings);

template
std::optional<ExternalDataSourceInfo> getExternalDataSourceConfiguration(
    const Poco::Util::AbstractConfiguration & dict_config, const String & dict_config_prefix,
    ContextPtr context, HasConfigKeyFunc has_config_key, const BaseSettings<EmptySettingsTraits> & settings);

template
SettingsChanges getSettingsChangesFromConfig(
    const BaseSettings<EmptySettingsTraits> & settings, const Poco::Util::AbstractConfiguration & config, const String & config_prefix);

#if USE_MYSQL
template
std::optional<ExternalDataSourceInfo> getExternalDataSourceConfiguration(
    const ASTList & args, ContextPtr context, bool is_database_engine, bool throw_on_no_collection, const BaseSettings<MySQLSettingsTraits> & storage_settings);

template
std::optional<ExternalDataSourceInfo> getExternalDataSourceConfiguration(
    const Poco::Util::AbstractConfiguration & dict_config, const String & dict_config_prefix,
    ContextPtr context, HasConfigKeyFunc has_config_key, const BaseSettings<MySQLSettingsTraits> & settings);

template
SettingsChanges getSettingsChangesFromConfig(
    const BaseSettings<MySQLSettingsTraits> & settings, const Poco::Util::AbstractConfiguration & config, const String & config_prefix);
#endif
}
