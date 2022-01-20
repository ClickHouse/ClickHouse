#include <TableFunctions/TableFunctionHiveCluster.h>
#if 0
#include <Common/Exception.h>
#include <Common/ErrorCodes.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Storage/Hive/StorageHive.h>

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int NOT_IMPLEMENTED;
}

void TableFunctionHiveCluster::parseArguments(const ASTPtr & ast_function_, ContextPtr context_)
{
    ASTs & args_func = ast_function_->children;
    if (args_func.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have arguments.", getName());
    
    ASTS & args = args_func.at(0)->children;

    const auto message = fmt::format(
        "The signature of function {} is:\n"\
        " - db, table",
        getName());
    
    if (args.size() != 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, message);
    
    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context_);
    
    db = args[0]->as<ASTLiteral &>().value.safeGet<String>();
    table = args[0]->as<ASTLiteral &>().value.safeGet<String>();

}

ColumnsDescription TableFunctionHiveCluster::getActualTableStructure(ContextPtr) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Table function {} doesn't implete getActualTableStructure", getName());
    return {};
}

StoragePtr TableFunctionHiveCluster::executeImpl(
    const ASTPtr & ast_function_, ContextPtr context_,
    const std::string & table_name_, ColumnsDescription /*cached_columns_*/) const
{
    StoragePtr storage;
    if (context_->getClientInfo().query_kind == )
}


void registerTableFunctionHiveCluster(TableFunctionFactory & factory_)
{
    factory_.registerFunction<TableFunctionHiveCluster>();
}

#endif
