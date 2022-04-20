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

void validateConfigKeys(
    const Poco::Util::AbstractConfiguration & dict_config, const String & config_prefix, const std::unordered_map<String, ConfigKeyInfo> & keys, const String & prefix)
{
    Poco::Util::AbstractConfiguration::Keys config_keys;
    dict_config.keys(config_prefix, config_keys);
    for (const auto & config_key : config_keys)
    {
        if (!keys.contains(config_key) && !config_key.starts_with(prefix))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected key `{}` found by path {}", config_key, config_prefix);
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

bool isNamedCollection(const ASTs & args, const Poco::Util::AbstractConfiguration & config)
{
    if (args.empty())
        return false;

    const auto * collection = typeid_cast<const ASTIdentifier *>(args[0].get());

    if (!collection)
        return false;

    return config.has("named_collections." + collection->name());
}

String getCollectionName(const ASTs & args)
{
    if (args.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not enough arguments, expected at least one");

    const auto * collection = assert_cast<const ASTIdentifier *>(args[0].get());
    return collection->name();
}

bool isNamedCollection(const Poco::Util::AbstractConfiguration & dict_config, const String & dict_config_prefix)
{
    return dict_config.has(dict_config_prefix + ".name");
}

String getCollectionName(const Poco::Util::AbstractConfiguration & dict_config, const String & dict_config_prefix)
{
    if (!isNamedCollection(dict_config, dict_config_prefix))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no collection");

    return dict_config.getString(dict_config_prefix + ".name");
}

ConfigurationFromNamedCollection parseConfigKeys(
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    const std::unordered_map<String, ConfigKeyInfo> & keys)
{
    ConfigurationFromNamedCollection configuration;

    for (const auto & [key, key_info] : keys)
    {
        Field value;
        auto path = config_prefix + "." + key;
        auto type = key_info.type;
        auto default_value = key_info.default_value;

        if (type == Field::Types::String)
            value = config.getString(path, default_value ? default_value->get<String>() : "");
        else if (type == Field::Types::Int64)
            value = config.getInt(path, default_value ? default_value->get<Int64>() : 0);
        else if (type == Field::Types::UInt64 || type == Field::Types::Bool)
            value = config.getUInt(path, default_value ? default_value->get<UInt64>() : 0);
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported type");

        configuration.emplace(key, value);
    }

    return configuration;
}

ConfigurationFromNamedCollection getConfigurationFromNamedCollection(
    const String & collection_name,
    const Poco::Util::AbstractConfiguration & config,
    const std::unordered_map<String, ConfigKeyInfo> & keys)
{
    ConfigurationFromNamedCollection configuration;

    const auto & collection_prefix = fmt::format("named_collections.{}", collection_name);

    if (!config.has(collection_prefix))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no collection named `{}` in config", collection_name);

    validateConfigKeys(config, collection_prefix, keys, "");

    return parseConfigKeys(config, collection_prefix, keys);
}

void overrideConfigurationFromNamedCollectionWithAST(
    ASTs & args,
    ConfigurationFromNamedCollection & configuration,
    const std::unordered_map<String, ConfigKeyInfo> & keys,
    ContextPtr context)
{
    for (size_t i = 1; i < args.size(); ++i)
    {
        if (const auto * ast_function = typeid_cast<const ASTFunction *>(args[i].get()))
        {
            const auto * args_expr = assert_cast<const ASTExpressionList *>(ast_function->arguments.get());
            auto function_args = args_expr->children;

            if (function_args.size() != 2)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected key-value defined argument");

            auto arg_name = function_args[0]->as<ASTIdentifier>()->name();

            if (!keys.contains(arg_name))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Key-value argument `{}` not found in the list of available keys", arg_name);

            auto arg_value_ast = evaluateConstantExpressionOrIdentifierAsLiteral(function_args[1], context);
            auto * arg_value_literal = arg_value_ast->as<ASTLiteral>();

            if (arg_value_literal)
                configuration[arg_name] = arg_value_literal->value;
        }
        else
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected key-value defined argument");
        }
    }
}

ListedConfigurationFromNamedCollection getListedConfigurationFromNamedCollection(
    const String & collection_name,
    const Poco::Util::AbstractConfiguration & config,
    const std::unordered_map<String, ConfigKeyInfo> & keys,
    const String & enumerate_by_key)
{
    ListedConfigurationFromNamedCollection configuration;

    const auto & collection_prefix = fmt::format("named_collections.{}", collection_name);
    validateConfigKeys(config, collection_prefix, keys, enumerate_by_key);

    auto root_configuration = getConfigurationFromNamedCollection(collection_name, config, keys);

    if (!config.has(collection_prefix + "." + enumerate_by_key))
        return ListedConfigurationFromNamedCollection{ .root_configuration = root_configuration };

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(collection_prefix, config_keys);

    ConfigurationsFromNamedCollection configurations;
    auto modified_keys{keys};
    for (const auto & config_key : config_keys)
    {
        if (config_key.starts_with(enumerate_by_key))
        {
            for (const auto & [key, value] : root_configuration)
                modified_keys.find(key)->second.default_value = value;

            String current_config_prefix = collection_prefix + "." + config_key;
            validateConfigKeys(config, current_config_prefix, keys, "");

            auto current_configuration = parseConfigKeys(config, current_config_prefix, modified_keys);
            configurations.push_back(std::move(current_configuration));
        }
    }

    return ListedConfigurationFromNamedCollection{ .root_configuration = root_configuration, .listed_configurations = std::move(configurations) };
}

String toString(const ConfigurationFromNamedCollection & configuration)
{
    WriteBufferFromOwnString out;
    for (auto it = configuration.begin(); it != configuration.end(); ++it)
    {
        const auto & [key, value] = *it;

        if (it != configuration.begin())
            out << ", ";

        out << key << " = " << toString(value);
    }
    return out.str();
}

template<typename T>
bool getExternalDataSourceConfiguration(const ASTs & args, BaseSettings<T> & settings, ContextPtr context)
{
    if (args.empty())
        return false;

    if (const auto * collection = typeid_cast<const ASTIdentifier *>(args[0].get()))
    {
        const auto & config = context->getConfigRef();
        const auto & config_prefix = fmt::format("named_collections.{}", collection->name());

        if (!config.has(config_prefix))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no collection named `{}` in config", collection->name());

        auto config_settings = getSettingsChangesFromConfig(settings, config, config_prefix);

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
bool getExternalDataSourceConfiguration(const ASTs & args, BaseSettings<RabbitMQSettingsTraits> & settings, ContextPtr context);
#endif

#if USE_RDKAFKA
template
bool getExternalDataSourceConfiguration(const ASTs & args, BaseSettings<KafkaSettingsTraits> & settings, ContextPtr context);
#endif

template
std::optional<ExternalDataSourceInfo> getExternalDataSourceConfiguration(
    const ASTs & args, ContextPtr context, bool is_database_engine, bool throw_on_no_collection, const BaseSettings<EmptySettingsTraits> & storage_settings);

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
    const ASTs & args, ContextPtr context, bool is_database_engine, bool throw_on_no_collection, const BaseSettings<MySQLSettingsTraits> & storage_settings);

template
std::optional<ExternalDataSourceInfo> getExternalDataSourceConfiguration(
    const Poco::Util::AbstractConfiguration & dict_config, const String & dict_config_prefix,
    ContextPtr context, HasConfigKeyFunc has_config_key, const BaseSettings<MySQLSettingsTraits> & settings);

template
SettingsChanges getSettingsChangesFromConfig(
    const BaseSettings<MySQLSettingsTraits> & settings, const Poco::Util::AbstractConfiguration & config, const String & config_prefix);
#endif
}
