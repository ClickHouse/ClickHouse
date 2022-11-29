#include <TableFunctions/TableFunctionExecutable.h>

#include <Common/Exception.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/StorageExecutable.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/interpretSubquery.h>
#include <boost/algorithm/string.hpp>
#include "registerTableFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNSUPPORTED_METHOD;
}

void TableFunctionExecutable::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto * function = ast_function->as<ASTFunction>();

    if (!function->arguments)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Table function '{}' must have arguments",
            getName());

    auto args = function->arguments->children;

    if (args.size() < 3)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Table function '{}' requires minimum 3 arguments: script_name, format, structure, [input_query...]",
            getName());

    for (size_t i = 0; i <= 2; ++i)
        args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(args[i], context);

    auto script_name_with_arguments_value = checkAndGetLiteralArgument<String>(args[0], "script_name_with_arguments_value");

    std::vector<String> script_name_with_arguments;
    boost::split(script_name_with_arguments, script_name_with_arguments_value, [](char c){ return c == ' '; });

    script_name = std::move(script_name_with_arguments[0]);
    script_name_with_arguments.erase(script_name_with_arguments.begin());
    arguments = std::move(script_name_with_arguments);
    format = checkAndGetLiteralArgument<String>(args[1], "format");
    structure = checkAndGetLiteralArgument<String>(args[2], "structure");

    for (size_t i = 3; i < args.size(); ++i)
    {
        if (args[i]->as<ASTSetQuery>())
        {
            settings_query = std::move(args[i]);
        }
        else
        {
            ASTPtr query = args[i]->children.at(0);
            if (query->as<ASTSelectWithUnionQuery>())
            {
                input_queries.emplace_back(std::move(query));
            }
            else
            {
                throw Exception(
                    ErrorCodes::UNSUPPORTED_METHOD,
                    "Table function '{}' argument is invalid {}",
                    getName(),
                    args[i]->formatForErrorMessage());
            }
        }
    }
}

ColumnsDescription TableFunctionExecutable::getActualTableStructure(ContextPtr context) const
{
    return parseColumnsListFromString(structure, context);
}

StoragePtr TableFunctionExecutable::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto storage_id = StorageID(getDatabaseName(), table_name);
    auto global_context = context->getGlobalContext();
    ExecutableSettings settings;
    settings.script_name = script_name;
    settings.script_arguments = arguments;
    if (settings_query != nullptr)
        settings.applyChanges(settings_query->as<ASTSetQuery>()->changes);

    auto storage = std::make_shared<StorageExecutable>(storage_id, format, settings, input_queries, getActualTableStructure(context), ConstraintsDescription{});
    storage->startup();
    return storage;
}

void registerTableFunctionExecutable(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionExecutable>();
}

}
