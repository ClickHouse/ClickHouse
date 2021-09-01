#include "ExternalDataSourceConfiguration.h"

#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

std::tuple<ExternalDataSourceConfiguration, EngineArgs, bool>
tryGetConfigurationAsNamedCollection(ASTs args, ContextPtr context)
{
    ExternalDataSourceConfiguration configuration;
    EngineArgs non_common_args;

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

        if ((args.size() == 1) && (configuration.host.empty() || configuration.port == 0
            || configuration.username.empty() || configuration.password.empty()
            || configuration.database.empty() || configuration.table.empty()))
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
                else
                    non_common_args.emplace_back(std::make_pair(arg_name, arg_value));
            }
            else
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected key-value defined argument");
            }
        }

        return std::make_tuple(configuration, non_common_args, true);
    }
    return std::make_tuple(configuration, non_common_args, false);
}

}
