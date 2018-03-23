#include <Common/config.h>
#if USE_MYSQL

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeString.h>
#include <Dictionaries/MySQLBlockInputStream.h>
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
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


DataTypePtr getDataType(const String & mysql_data_type, bool is_unsigned, size_t length)
{
    if (mysql_data_type == "tinyint")
    {
        if (is_unsigned)
            return std::make_shared<DataTypeUInt8>();
        else
            return std::make_shared<DataTypeInt8>();
    }
    if (mysql_data_type == "smallint")
    {
        if (is_unsigned)
            return std::make_shared<DataTypeUInt16>();
        else
            return std::make_shared<DataTypeInt16>();
    }
    if (mysql_data_type == "int" || mysql_data_type == "mediumint")
    {
        if (is_unsigned)
            return std::make_shared<DataTypeUInt32>();
        else
            return std::make_shared<DataTypeInt32>();
    }
    if (mysql_data_type == "bigint")
    {
        if (is_unsigned)
            return std::make_shared<DataTypeUInt64>();
        else
            return std::make_shared<DataTypeInt64>();
    }
    if (mysql_data_type == "float")
        return std::make_shared<DataTypeFloat32>();
    if (mysql_data_type == "double")
        return std::make_shared<DataTypeFloat64>();
    if (mysql_data_type == "date")
        return std::make_shared<DataTypeDate>();
    if (mysql_data_type == "datetime" || mysql_data_type == "timestamp")
        return std::make_shared<DataTypeDateTime>();
    if (mysql_data_type == "binary")
        return std::make_shared<DataTypeFixedString>(length);

    /// Also String is fallback for all unknown types.
    return std::make_shared<DataTypeString>();
}


StoragePtr TableFunctionMySQL::executeImpl(const ASTPtr & ast_function, const Context & context) const
{
    const ASTFunction & args_func = typeid_cast<const ASTFunction &>(*ast_function);

    if (!args_func.arguments)
        throw Exception("Table function 'mysql' must have arguments.", ErrorCodes::LOGICAL_ERROR);

    ASTs & args = typeid_cast<ASTExpressionList &>(*args_func.arguments).children;

    if (args.size() != 5)
        throw Exception("Table function 'mysql' requires exactly 5 arguments: host:port, database name, table name, username and password",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (size_t i = 0; i < 5; ++i)
        args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(args[i], context);

    std::string host_port = static_cast<const ASTLiteral &>(*args[0]).value.safeGet<String>();
    std::string database_name = static_cast<const ASTLiteral &>(*args[1]).value.safeGet<String>();
    std::string table_name = static_cast<const ASTLiteral &>(*args[2]).value.safeGet<String>();
    std::string user_name = static_cast<const ASTLiteral &>(*args[3]).value.safeGet<String>();
    std::string password = static_cast<const ASTLiteral &>(*args[4]).value.safeGet<String>();

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
                    (*block.getByPosition(3).column)[i].safeGet<UInt64>(),
                    (*block.getByPosition(4).column)[i].safeGet<UInt64>()));

        /// TODO is_nullable is ignored.
    }

    auto res = StorageMySQL::create(
        table_name,
        std::move(pool),
        database_name,
        table_name,
        ColumnsDescription{columns});

    res->startup();
    return res;
}


void registerTableFunctionMySQL(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionMySQL>();
}
}

#endif
