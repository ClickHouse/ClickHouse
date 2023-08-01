#include <Interpreters/Context.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/StorageNull.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionNull.h>
#include <Interpreters/evaluateConstantExpression.h>
#include "registerTableFunctions.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

void TableFunctionNull::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto * function = ast_function->as<ASTFunction>();
    if (!function || !function->arguments)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' requires 'structure'", getName());

    const auto & arguments = function->arguments->children;
    if (!arguments.empty() && arguments.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Table function '{}' requires 'structure' argument or empty argument", getName());

    if (!arguments.empty())
        structure = checkAndGetLiteralArgument<String>(evaluateConstantExpressionOrIdentifierAsLiteral(arguments[0], context), "structure");
}

ColumnsDescription TableFunctionNull::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    if (structure != "auto")
        return parseColumnsListFromString(structure, context);
    return default_structure;
}

StoragePtr TableFunctionNull::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/, bool /*is_insert_query*/) const
{
    ColumnsDescription columns;
    if (structure != "auto")
        columns = parseColumnsListFromString(structure, context);
    else if (!structure_hint.empty())
        columns = structure_hint;
    else
        columns = default_structure;

    auto res = std::make_shared<StorageNull>(StorageID(getDatabaseName(), table_name), columns, ConstraintsDescription(), String{});
    res->startup();
    return res;
}

void registerTableFunctionNull(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionNull>({.documentation = {}, .allow_readonly = true});
}
}
