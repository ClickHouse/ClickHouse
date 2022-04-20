#include "NamedCollections.h"

#include <Common/assert_cast.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>


namespace DB
{

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

}
