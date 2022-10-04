#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageFilesystem.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionFilesystem.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

void registerTableFunctionFilesystem(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionFilesystem>();
}
void TableFunctionFilesystem::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    /// Parse args
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception("Table function '" + getName() + "' must have arguments.", ErrorCodes::LOGICAL_ERROR);

    ASTs & args = args_func.at(0)->children;

    if (args.empty())
        throw Exception("Table function '" + getName() + "' requires at least 1 argument", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    path = args[0]->as<ASTLiteral &>().value.safeGet<String>();

    if (args.size() > 1)
        throw Exception("Table function '" + getName() + "' requires path", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
}
StoragePtr TableFunctionFilesystem::executeImpl(const ASTPtr &, ContextPtr, const std::string & table_name, ColumnsDescription) const
{
    StoragePtr res = std::make_shared<StorageFilesystem>(
        StorageID(getDatabaseName(), table_name), structure, path, ConstraintsDescription(), String{});
    res->startup();
    return res;
}
}
