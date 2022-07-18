#include <TableFunctions/TableFunctionExecutable.h>

#include <Common/Exception.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
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

    auto scipt_name_with_arguments_value = args[0]->as<ASTLiteral &>().value.safeGet<String>();

    std::vector<String> script_name_with_arguments;
    boost::split(script_name_with_arguments, scipt_name_with_arguments_value, [](char c){ return c == ' '; });

    script_name = script_name_with_arguments[0];
    script_name_with_arguments.erase(script_name_with_arguments.begin());
    arguments = std::move(script_name_with_arguments);
    format = args[1]->as<ASTLiteral &>().value.safeGet<String>();
    structure = args[2]->as<ASTLiteral &>().value.safeGet<String>();

    for (size_t i = 3; i < args.size(); ++i)
    {
        ASTPtr query = args[i]->children.at(0);
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
