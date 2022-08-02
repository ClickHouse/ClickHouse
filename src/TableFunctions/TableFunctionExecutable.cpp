#include <TableFunctions/TableFunctionExecutable.h>

#include <Common/Exception.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
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

    auto it = args.begin();
    *it = evaluateConstantExpressionOrIdentifierAsLiteral(*it, context);
    ++it;
    *it = evaluateConstantExpressionOrIdentifierAsLiteral(*it, context);

    it = args.begin();
    auto script_name_with_arguments_value = checkAndGetLiteralArgument<String>((*it++), "script_name_with_arguments_value");

    std::vector<String> script_name_with_arguments;
    boost::split(script_name_with_arguments, script_name_with_arguments_value, [](char c){ return c == ' '; });

    script_name = script_name_with_arguments[0];
    script_name_with_arguments.erase(script_name_with_arguments.begin());
    arguments = std::move(script_name_with_arguments);
    format = checkAndGetLiteralArgument<String>((*it++), "format");
    structure = checkAndGetLiteralArgument<String>((*it++), "structure");

    for (; it != args.end(); ++it)
    {
        ASTPtr query = (*it)->children.front();
        if (!query->as<ASTSelectWithUnionQuery>())
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Table function '{}' argument is invalid input query {}",
                getName(),
                query->formatForErrorMessage());

        input_queries.emplace_back(std::move(query));
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

    auto storage = std::make_shared<StorageExecutable>(storage_id, format, settings, input_queries, getActualTableStructure(context), ConstraintsDescription{});
    storage->startup();
    return storage;
}

void registerTableFunctionExecutable(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionExecutable>();
}

}
