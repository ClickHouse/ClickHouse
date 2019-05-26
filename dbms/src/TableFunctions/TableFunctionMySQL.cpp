#include <Common/config.h>
#if USE_MYSQL

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeNullable.h>
#include <Formats/MySQLBlockInputStream.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageMySQL.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionMySQL.h>
#include <Core/Defines.h>
#include <Common/Exception.h>
#include <Common/parseAddress.h>
#include <Common/typeid_cast.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <mysqlxx/Pool.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}


DataTypePtr getDataType(const String & mysql_data_type, bool is_nullable, bool is_unsigned, size_t length)
{
    DataTypePtr res;
    if (mysql_data_type == "tinyint")
    {
        if (is_unsigned)
            res = std::make_shared<DataTypeUInt8>();
        else
            res = std::make_shared<DataTypeInt8>();
    }
    else if (mysql_data_type == "smallint")
    {
        if (is_unsigned)
            res = std::make_shared<DataTypeUInt16>();
        else
            res = std::make_shared<DataTypeInt16>();
    }
    else if (mysql_data_type == "int" || mysql_data_type == "mediumint")
    {
        if (is_unsigned)
            res = std::make_shared<DataTypeUInt32>();
        else
            res = std::make_shared<DataTypeInt32>();
    }
    else if (mysql_data_type == "bigint")
    {
        if (is_unsigned)
            res = std::make_shared<DataTypeUInt64>();
        else
            res = std::make_shared<DataTypeInt64>();
    }
    else if (mysql_data_type == "float")
        res = std::make_shared<DataTypeFloat32>();
    else if (mysql_data_type == "double")
        res = std::make_shared<DataTypeFloat64>();
    else if (mysql_data_type == "date")
        res = std::make_shared<DataTypeDate>();
    else if (mysql_data_type == "datetime" || mysql_data_type == "timestamp")
        res = std::make_shared<DataTypeDateTime>();
    else if (mysql_data_type == "binary")
        res = std::make_shared<DataTypeFixedString>(length);
    else
        /// Also String is fallback for all unknown types.
        res = std::make_shared<DataTypeString>();
    if (is_nullable)
        res = std::make_shared<DataTypeNullable>(res);
    return res;
}


StoragePtr TableFunctionMySQL::executeImpl(const ASTPtr & ast_function, const Context & context) const
{
    const auto & args_func = ast_function->as<ASTFunction &>();

    if (!args_func.arguments)
        throw Exception("Table function 'mysql' must have arguments.", ErrorCodes::LOGICAL_ERROR);

    ASTs & args = args_func.arguments->children;

    if (args.size() < 5 || args.size() > 7)
        throw Exception("Table function 'mysql' requires 5-7 parameters: MySQL('host:port', database, table, 'user', 'password'[, replace_query, 'on_duplicate_clause']).",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (size_t i = 0; i < args.size(); ++i)
        args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(args[i], context);

    std::string host_port = args[0]->as<ASTLiteral &>().value.safeGet<String>();
    std::string database_name = args[1]->as<ASTLiteral &>().value.safeGet<String>();
    std::string table_name = args[2]->as<ASTLiteral &>().value.safeGet<String>();
    std::string user_name = args[3]->as<ASTLiteral &>().value.safeGet<String>();
    std::string password = args[4]->as<ASTLiteral &>().value.safeGet<String>();

    bool replace_query = false;
    std::string on_duplicate_clause;
    if (args.size() >= 6)
        replace_query = args[5]->as<ASTLiteral &>().value.safeGet<UInt64>() > 0;
    if (args.size() == 7)
        on_duplicate_clause = args[6]->as<ASTLiteral &>().value.safeGet<String>();

    if (replace_query && !on_duplicate_clause.empty())
        throw Exception(
            "Only one of 'replace_query' and 'on_duplicate_clause' can be specified, or none of them",
            ErrorCodes::BAD_ARGUMENTS);

    /// 3306 is the default MySQL port number
    auto parsed_host_port = parseAddress(host_port, 3306);

    mysqlxx::Pool pool(database_name, parsed_host_port.first, user_name, password, parsed_host_port.second);

    /// Determine table definition by running a query to INFORMATION_SCHEMA.

    Block sample_block
    {
        { std::make_shared<DataTypeString>(), "name" },
        { std::make_shared<DataTypeString>(), "type" },
        { std::make_shared<DataTypeUInt8>(), "is_nullable" },
        { std::make_shared<DataTypeUInt8>(), "is_unsigned" },
        { std::make_shared<DataTypeUInt64>(), "length" },
    };

    WriteBufferFromOwnString query;
    query << "SELECT"
            " COLUMN_NAME AS name,"
            " DATA_TYPE AS type,"
            " IS_NULLABLE = 'YES' AS is_nullable,"
            " COLUMN_TYPE LIKE '%unsigned' AS is_unsigned,"
            " CHARACTER_MAXIMUM_LENGTH AS length"
        " FROM INFORMATION_SCHEMA.COLUMNS"
        " WHERE TABLE_SCHEMA = " << quote << database_name
        << " AND TABLE_NAME = " << quote << table_name
        << " ORDER BY ORDINAL_POSITION";

    MySQLBlockInputStream result(pool.Get(), query.str(), sample_block, DEFAULT_BLOCK_SIZE);

    NamesAndTypesList columns;
    while (Block block = result.read())
    {
        size_t rows = block.rows();
        for (size_t i = 0; i < rows; ++i)
            columns.emplace_back(
                (*block.getByPosition(0).column)[i].safeGet<String>(),
                getDataType(
                    (*block.getByPosition(1).column)[i].safeGet<String>(),
                    (*block.getByPosition(2).column)[i].safeGet<UInt64>() && context.getSettings().external_table_functions_use_nulls,
                    (*block.getByPosition(3).column)[i].safeGet<UInt64>(),
                    (*block.getByPosition(4).column)[i].safeGet<UInt64>()));

    }

    auto res = StorageMySQL::create(
        table_name,
        std::move(pool),
        database_name,
        table_name,
        replace_query,
        on_duplicate_clause,
        ColumnsDescription{columns},
        context);

    res->startup();
    return res;
}


void registerTableFunctionMySQL(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionMySQL>();
}
}

#endif
