#include "ExternalDataSourceConfiguration.h"

#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <IO/WriteBufferFromString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
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
}


std::optional<ExternalDataSourceConfig> getExternalDataSourceConfiguration(const ASTs & args, ContextPtr context, bool is_database_engine)
{
    if (args.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "External data source must have arguments");

    ExternalDataSourceConfiguration configuration;
    StorageSpecificArgs non_common_args;

    if (const auto * collection = typeid_cast<const ASTIdentifier *>(args[0].get()))
    {
        const auto & config = context->getConfigRef();
        const auto & config_prefix = fmt::format("named_collections.{}", collection->name());

        if (!config.has(config_prefix))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no collection named `{}` in config", collection->name());

        configuration.host = config.getString(config_prefix + ".host", "");
        configuration.port = config.getInt(config_prefix + ".port", 0);
        configuration.username = config.getString(config_prefix + ".user", "");
        configuration.password = config.getString(config_prefix + ".password", "");
        configuration.database = config.getString(config_prefix + ".database", "");
        configuration.table = config.getString(config_prefix + ".table", "");
        configuration.schema = config.getString(config_prefix + ".schema", "");

        if ((args.size() == 1) && (configuration.host.empty() || configuration.port == 0
            || configuration.username.empty() || configuration.password.empty()
            || configuration.database.empty() || (configuration.table.empty() && !is_database_engine)))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Named collection of connection parameters is missing some of the parameters and no key-value arguments are added");
        }

        for (size_t i = 1; i < args.size(); ++i)
        {
            if (const auto * ast_function = typeid_cast<const ASTFunction *>(args[i].get()))
            {
                const auto * args_expr = assert_cast<const ASTExpressionList *>(ast_function->arguments.get());
                auto function_args = args_expr->children;
                if (function_args.size() != 2)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected key-value defined argument");

                auto arg_name = function_args[0]->as<ASTIdentifier>()->name();
                auto arg_value = evaluateConstantExpressionOrIdentifierAsLiteral(function_args[1], context)->as<ASTLiteral>()->value;

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
                else
                    non_common_args.emplace_back(std::make_pair(arg_name, arg_value));
            }
            else
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected key-value defined argument");
            }
        }

        ExternalDataSourceConfig source_config{ .configuration = configuration, .specific_args = non_common_args };
        return source_config;
    }
    return std::nullopt;
}


std::optional<ExternalDataSourceConfiguration> getExternalDataSourceConfiguration(
    const Poco::Util::AbstractConfiguration & dict_config, const String & dict_config_prefix, ContextPtr context)
{
    ExternalDataSourceConfiguration configuration;

    auto collection_name = dict_config.getString(dict_config_prefix + ".name", "");
    if (!collection_name.empty())
    {
        const auto & config = context->getConfigRef();
        const auto & config_prefix = fmt::format("named_collections.{}", collection_name);

        if (!config.has(config_prefix))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no collection named `{}` in config", collection_name);

        configuration.host = dict_config.getString(dict_config_prefix + ".host", config.getString(config_prefix + ".host", ""));
        configuration.port = dict_config.getInt(dict_config_prefix + ".port", config.getUInt(config_prefix + ".port", 0));
        configuration.username = dict_config.getString(dict_config_prefix + ".user", config.getString(config_prefix + ".user", ""));
        configuration.password = dict_config.getString(dict_config_prefix + ".password", config.getString(config_prefix + ".password", ""));
        configuration.database = dict_config.getString(dict_config_prefix + ".db", config.getString(config_prefix + ".database", ""));
        configuration.table = dict_config.getString(dict_config_prefix + ".table", config.getString(config_prefix + ".table", ""));
        configuration.schema = dict_config.getString(dict_config_prefix + ".schema", config.getString(config_prefix + ".schema", ""));

        if (configuration.host.empty() || configuration.port == 0 || configuration.username.empty() || configuration.password.empty()
            || configuration.database.empty() || configuration.table.empty())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Named collection of connection parameters is missing some of the parameters and dictionary parameters are added");
        }
        return configuration;
    }
    return std::nullopt;
}


ExternalDataSourcesByPriority getExternalDataSourceConfigurationByPriority(
    const Poco::Util::AbstractConfiguration & dict_config, const String & dict_config_prefix, ContextPtr context)
{
    ExternalDataSourceConfiguration common_configuration;

    auto named_collection = getExternalDataSourceConfiguration(dict_config, dict_config_prefix, context);
    if (named_collection)
    {
        common_configuration = *named_collection;
    }
    else
    {
        common_configuration.host = dict_config.getString(dict_config_prefix + ".host", "");
        common_configuration.port = dict_config.getUInt(dict_config_prefix + ".port", 0);
        common_configuration.username = dict_config.getString(dict_config_prefix + ".user", "");
        common_configuration.password = dict_config.getString(dict_config_prefix + ".password", "");
        common_configuration.database = dict_config.getString(dict_config_prefix + ".db", "");
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
                non_common_args.emplace_back(std::make_pair(key, config.getString(config_prefix + '.' + key)));
        }

        for (size_t i = 1; i < args.size(); ++i)
        {
            if (const auto * ast_function = typeid_cast<const ASTFunction *>(args[i].get()))
            {
                const auto * args_expr = assert_cast<const ASTExpressionList *>(ast_function->arguments.get());
                auto function_args = args_expr->children;
                if (function_args.size() != 2)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected key-value defined argument");

                auto arg_name = function_args[0]->as<ASTIdentifier>()->name();
                auto arg_value = evaluateConstantExpressionOrIdentifierAsLiteral(function_args[1], context)->as<ASTLiteral>()->value;

                if (arg_name == "url")
                    configuration.url = arg_value.safeGet<String>();
                else if (arg_name == "format")
                    configuration.format = arg_value.safeGet<String>();
                else if (arg_name == "compression_method")
                    configuration.compression_method = arg_value.safeGet<String>();
                else if (arg_name == "structure")
                    configuration.structure = arg_value.safeGet<String>();
                else
                    non_common_args.emplace_back(std::make_pair(arg_name, arg_value));
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

}
