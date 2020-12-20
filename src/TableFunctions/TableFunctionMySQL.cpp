#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL
#    include <Core/Defines.h>
#    include <Databases/MySQL/FetchTablesColumnsList.h>
#    include <DataTypes/DataTypeString.h>
#    include <DataTypes/DataTypesNumber.h>
#    include <DataTypes/convertMySQLDataType.h>
#    include <Formats/MySQLBlockInputStream.h>
#    include <IO/Operators.h>
#    include <Interpreters/Context.h>
#    include <Interpreters/evaluateConstantExpression.h>
#    include <Parsers/ASTFunction.h>
#    include <Parsers/ASTLiteral.h>
#    include <Storages/StorageMySQL.h>
#    include <TableFunctions/ITableFunction.h>
#    include <TableFunctions/TableFunctionFactory.h>
#    include <TableFunctions/TableFunctionMySQL.h>
#    include <Common/Exception.h>
#    include <Common/parseAddress.h>
#    include <Common/quoteString.h>
#    include "registerTableFunctions.h"

#    include <Databases/MySQL/DatabaseConnectionMySQL.h> // for fetchTablesColumnsList


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_TABLE;
}

void TableFunctionMySQL::parseArguments(const ASTPtr & ast_function, const Context & context)
{
    const auto & args_func = ast_function->as<ASTFunction &>();

    if (!args_func.arguments)
        throw Exception("Table function 'mysql' must have arguments.", ErrorCodes::LOGICAL_ERROR);

    ASTs & args = args_func.arguments->children;

    if (args.size() < 5 || args.size() > 7)
        throw Exception("Table function 'mysql' requires 5-7 parameters: MySQL('host:port', database, table, 'user', 'password'[, replace_query, 'on_duplicate_clause']).",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    String host_port = args[0]->as<ASTLiteral &>().value.safeGet<String>();
    remote_database_name = args[1]->as<ASTLiteral &>().value.safeGet<String>();
    remote_table_name = args[2]->as<ASTLiteral &>().value.safeGet<String>();
    user_name = args[3]->as<ASTLiteral &>().value.safeGet<String>();
    password = args[4]->as<ASTLiteral &>().value.safeGet<String>();

    if (args.size() >= 6)
        replace_query = args[5]->as<ASTLiteral &>().value.safeGet<UInt64>() > 0;
    if (args.size() == 7)
        on_duplicate_clause = args[6]->as<ASTLiteral &>().value.safeGet<String>();

    if (replace_query && !on_duplicate_clause.empty())
        throw Exception(
            "Only one of 'replace_query' and 'on_duplicate_clause' can be specified, or none of them",
            ErrorCodes::BAD_ARGUMENTS);

    /// 3306 is the default MySQL port number
    parsed_host_port = parseAddress(host_port, 3306);
}

ColumnsDescription TableFunctionMySQL::getActualTableStructure(const Context & context) const
{
    assert(!parsed_host_port.first.empty());
    if (!pool)
        pool.emplace(remote_database_name, parsed_host_port.first, user_name, password, parsed_host_port.second);

    const auto & settings = context.getSettingsRef();
    const auto tables_and_columns = fetchTablesColumnsList(*pool, remote_database_name, {remote_table_name}, settings.external_table_functions_use_nulls, settings.mysql_datatypes_support_level);

    const auto columns = tables_and_columns.find(remote_table_name);
    if (columns == tables_and_columns.end())
        throw Exception("MySQL table " + backQuoteIfNeed(remote_database_name) + "." + backQuoteIfNeed(remote_table_name) + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);

    return ColumnsDescription{columns->second};
}

StoragePtr TableFunctionMySQL::executeImpl(const ASTPtr & /*ast_function*/, const Context & context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    assert(!parsed_host_port.first.empty());
    if (!pool)
        pool.emplace(remote_database_name, parsed_host_port.first, user_name, password, parsed_host_port.second);

    auto columns = getActualTableStructure(context);

    auto res = StorageMySQL::create(
        StorageID(getDatabaseName(), table_name),
        std::move(*pool),
        remote_database_name,
        remote_table_name,
        replace_query,
        on_duplicate_clause,
        columns,
        ConstraintsDescription{},
        context);

    pool.reset();

    res->startup();
    return res;
}


void registerTableFunctionMySQL(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionMySQL>();
}
}

#endif
