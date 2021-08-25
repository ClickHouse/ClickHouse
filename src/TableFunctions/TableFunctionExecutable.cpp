#include <TableFunctions/TableFunctionExecutable.h>

#include <Common/Exception.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/parseQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
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

    if (!(args.size() == 3 || args.size() == 4))
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Table function '{}' requires minimum 3 arguments: script_name, format, structure, [input_query...]",
            getName());

    for (size_t i = 0; i <= 2; ++i)
        args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(args[i], context);

    file_path = args[0]->as<ASTLiteral &>().value.safeGet<String>();
    format = args[1]->as<ASTLiteral &>().value.safeGet<String>();
    structure = evaluateConstantExpressionOrIdentifierAsLiteral(args[2], context)->as<ASTLiteral &>().value.safeGet<String>();

    for (size_t i = 3; i < args.size(); ++i)
    {
        auto query_value = args[i]->as<ASTLiteral &>().value.safeGet<String>();

        ParserSelectWithUnionQuery parser;
        ASTPtr query = parseQuery(parser, query_value, 1000, 1000);

        if (!query)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Table function '{}' argument is invalid input query {}",
                getName(),
                query_value);

        InterpreterSelectWithUnionQuery interpreter(query, context, {});
        auto input = interpreter.execute().getInputStream();
        inputs.emplace_back(std::move(input));
    }
}

ColumnsDescription TableFunctionExecutable::getActualTableStructure(ContextPtr context) const
{
    return parseColumnsListFromString(structure, context);
}

StoragePtr TableFunctionExecutable::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto storage_id = StorageID(getDatabaseName(), table_name);
    auto storage = StorageExecutable::create(storage_id, file_path, format, inputs, getActualTableStructure(context), ConstraintsDescription{});
    storage->startup();
    return storage;
}

void registerTableFunctionExecutable(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionExecutable>();
}

}
