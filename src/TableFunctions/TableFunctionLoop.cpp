#include "config.h"
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Storages/StorageMaterializedView.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTFunction.h>
#include <Common/Exception.h>
#include <Common/parseAddress.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Common/quoteString.h>
#include <Parsers/ASTLiteral.h>
#include "registerTableFunctions.h"

#include <Common/parseRemoteDescription.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int UNKNOWN_TABLE;
}
namespace
{
class TableFunctionLoop : public ITableFunction{
public:
    static constexpr auto name = "loop";
    std::string getName() const override { return name; }
private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const String & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;
    const char * getStorageTypeName() const override { return "Loop"; }
    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    // save the inner table function AST
    ASTPtr inner_table_function_ast;
    // save database and table
    std::string database_name_;
    std::string table_name_;
};

}

void TableFunctionLoop::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & args_func = ast_function->as<ASTFunction &>();

    if (!args_func.arguments)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function 'loop' must have arguments.");

    auto & args = args_func.arguments->children;
    if (args.empty())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "No arguments provided for table function 'loop'");

    // loop(database, table)
    if (args.size() == 2)
    {
        args[0] = evaluateConstantExpressionForDatabaseName(args[0], context);
        args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(args[1], context);

        database_name_ = checkAndGetLiteralArgument<String>(args[0], "database");
        table_name_ = checkAndGetLiteralArgument<String>(args[1], "table");
        /*if (const auto * lit = args[0]->as<ASTLiteral>())
            database_name_ = lit->value.safeGet<String>();
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Expected literal for argument 1 of function 'loop', got {}", args[0]->getID());

        if (const auto * lit = args[1]->as<ASTLiteral>())
            table_name_ = lit->value.safeGet<String>();
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Expected literal for argument 2 of function 'loop', got {}", args[1]->getID());*/
    }
    // loop(other_table_function(...))
    else if (args.size() == 1)
        inner_table_function_ast = args[0];

    else
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function 'loop' must have 1 or 2 arguments.");
}

ColumnsDescription TableFunctionLoop::getActualTableStructure(ContextPtr context, bool is_insert_query) const
{
    auto inner_table_function = TableFunctionFactory::instance().get(inner_table_function_ast, context);

    return inner_table_function->getActualTableStructure(context, is_insert_query);

}

StoragePtr TableFunctionLoop::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription cached_columns,
    bool is_insert_query) const
{
    StoragePtr storage;
    if (!database_name_.empty() && !table_name_.empty())
    {
        auto database = DatabaseCatalog::instance().getDatabase(database_name_);
        storage = database->tryGetTable(table_name_ ,context);
        if (!storage)
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table '{}' not found in database '{}'", table_name_, database_name_);
    }
    else
    {
        auto inner_table_function = TableFunctionFactory::instance().get(inner_table_function_ast, context);
        storage = inner_table_function->execute(
            inner_table_function_ast,
            context,
            table_name,
            std::move(cached_columns),
            is_insert_query);
    }

    return storage;
}

void registerTableFunctionLoop(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionLoop>();
}

}
