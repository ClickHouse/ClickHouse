#include "config.h"

#if USE_SQLITE

#include <Common/Exception.h>
#include <TableFunctions/ITableFunction.h>
#include <Storages/StorageSQLite.h>

#include <Databases/SQLite/SQLiteUtils.h>
#include "registerTableFunctions.h"

#include <Interpreters/evaluateConstantExpression.h>

#include <Parsers/ASTFunction.h>

#include <TableFunctions/TableFunctionFactory.h>

#include <Storages/checkAndGetLiteralArgument.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

namespace
{

class TableFunctionSQLite : public ITableFunction
{
public:
    static constexpr auto name = "sqlite";
    std::string getName() const override { return name; }

private:
    StoragePtr executeImpl(
            const ASTPtr & ast_function, ContextPtr context,
            const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;

    const char * getStorageTypeName() const override { return "SQLite"; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    String database_path, remote_table_name;
    std::shared_ptr<sqlite3> sqlite_db;
};

StoragePtr TableFunctionSQLite::executeImpl(const ASTPtr & /*ast_function*/,
        ContextPtr context, const String & table_name, ColumnsDescription cached_columns, bool /*is_insert_query*/) const
{
    auto storage = std::make_shared<StorageSQLite>(StorageID(getDatabaseName(), table_name),
                                         sqlite_db,
                                         database_path,
                                         remote_table_name,
                                         cached_columns, ConstraintsDescription{}, /* comment = */ "", context);

    storage->startup();
    return storage;
}


ColumnsDescription TableFunctionSQLite::getActualTableStructure(ContextPtr /* context */, bool /*is_insert_query*/) const
{
    return StorageSQLite::getTableStructureFromData(sqlite_db, remote_table_name);
}


void TableFunctionSQLite::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & func_args = ast_function->as<ASTFunction &>();

    if (!func_args.arguments)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function 'sqlite' must have arguments.");

    ASTs & args = func_args.arguments->children;

    if (args.size() != 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "SQLite database requires 2 arguments: database path, table name");

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    database_path = checkAndGetLiteralArgument<String>(args[0], "database_path");
    remote_table_name = checkAndGetLiteralArgument<String>(args[1], "table_name");

    sqlite_db = openSQLiteDB(database_path, context);
}

}

void registerTableFunctionSQLite(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionSQLite>();
}

}

#endif
