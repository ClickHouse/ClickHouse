#include <TableFunctions/TableFunctionPostgreSQL.h>

#if USE_LIBPQXX
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/Exception.h>
#include <Common/parseAddress.h>
#include "registerTableFunctions.h"
#include <Storages/StoragePostgreSQL.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_TYPE;
}


StoragePtr TableFunctionPostgreSQL::executeImpl(const ASTPtr & /*ast_function*/,
        const Context & context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto columns = getActualTableStructure(context);
    auto result = std::make_shared<StoragePostgreSQL>(
            StorageID(getDatabaseName(), table_name), remote_table_name,
            connection, connection_str, columns, ConstraintsDescription{}, context);

    result->startup();
    return result;
}


ColumnsDescription TableFunctionPostgreSQL::getActualTableStructure(const Context & context) const
{
    const bool use_nulls = context.getSettingsRef().external_table_functions_use_nulls;
    auto columns = NamesAndTypesList();

    std::string query = fmt::format(
           "SELECT attname AS name, format_type(atttypid, atttypmod) AS type, "
           "attnotnull AS not_null, attndims AS dims "
           "FROM pg_attribute "
           "WHERE attrelid = '{}'::regclass "
           "AND NOT attisdropped AND attnum > 0", remote_table_name);
    pqxx::read_transaction tx(*connection);
    pqxx::stream_from stream(tx, pqxx::from_query, std::string_view(query));
    std::tuple<std::string, std::string, std::string, uint16_t> row;

    while (stream >> row)
    {
        columns.push_back(NameAndTypePair(
                std::get<0>(row),
                getDataType(std::get<1>(row), use_nulls && (std::get<2>(row) == "f"), std::get<3>(row))));
    }
    stream.complete();
    tx.commit();

    return ColumnsDescription{columns};
}


DataTypePtr TableFunctionPostgreSQL::getDataType(std::string & type, bool is_nullable, uint16_t dimensions) const
{
    DataTypePtr res;

    if (dimensions)
    {
        /// No matter how many dimensions, in type we will get only one '[]' (i.e. Integer[])
        type.resize(type.size() - 2);
    }

    if (type == "smallint")
        res = std::make_shared<DataTypeInt16>();
    else if (type == "integer")
        res = std::make_shared<DataTypeInt32>();
    else if (type == "bigint")
        res = std::make_shared<DataTypeInt64>();
    else if (type == "real")
        res = std::make_shared<DataTypeFloat32>();
    else if (type == "double precision")
        res = std::make_shared<DataTypeFloat64>();
    else if (type == "serial")
        res = std::make_shared<DataTypeUInt32>();
    else if (type == "bigserial")
        res = std::make_shared<DataTypeUInt64>();
    else if (type.starts_with("timestamp"))
        res = std::make_shared<DataTypeDateTime>();
    else if (type == "date")
        res = std::make_shared<DataTypeDate>();
    else if (type.starts_with("numeric"))
    {
        /// Numeric and decimal will both be numeric
        /// Will get numeric(precision, scale) string, need to extract precision and scale
        std::vector<std::string> result;
        boost::split(result, type, [](char c){ return c == '(' || c == ',' || c == ')'; });
        for (std::string & key : result)
            boost::trim(key);

        /// If precision or scale are not specified, postgres creates a column in which numeric values of
        /// any precision and scale can be stored, so may be maxPrecision may be used instead of exception
        if (result.size() < 3)
            throw Exception("Numeric lacks precision and scale in its definition", ErrorCodes::UNKNOWN_TYPE);

        uint32_t precision = std::atoi(result[1].data());
        uint32_t scale = std::atoi(result[2].data());

        if (precision <= DecimalUtils::maxPrecision<Decimal32>())
            res = std::make_shared<DataTypeDecimal<Decimal32>>(precision, scale);
        else if (precision <= DecimalUtils::maxPrecision<Decimal64>())
            res = std::make_shared<DataTypeDecimal<Decimal64>>(precision, scale);
        else if (precision <= DecimalUtils::maxPrecision<Decimal128>())
            res = std::make_shared<DataTypeDecimal<Decimal128>>(precision, scale);
    }

    if (!res)
        res = std::make_shared<DataTypeString>();
    if (is_nullable)
        res = std::make_shared<DataTypeNullable>(res);
    while (dimensions--)
        res = std::make_shared<DataTypeArray>(res);

    return res;
}


void TableFunctionPostgreSQL::parseArguments(const ASTPtr & ast_function, const Context & context)
{
    const auto & func_args = ast_function->as<ASTFunction &>();

    if (!func_args.arguments)
        throw Exception("Table function 'PostgreSQL' must have arguments.", ErrorCodes::BAD_ARGUMENTS);

    ASTs & args = func_args.arguments->children;

    if (args.size() < 5)
        throw Exception("Table function 'PostgreSQL' requires 5 parameters: "
                        "PostgreSQL('host:port', 'database', 'table', 'user', 'password').",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    auto parsed_host_port = parseAddress(args[0]->as<ASTLiteral &>().value.safeGet<String>(), 5432);
    remote_table_name = args[2]->as<ASTLiteral &>().value.safeGet<String>();

    connection_str = fmt::format("dbname={} host={} port={} user={} password={}",
            args[1]->as<ASTLiteral &>().value.safeGet<String>(),
            parsed_host_port.first, std::to_string(parsed_host_port.second),
            args[3]->as<ASTLiteral &>().value.safeGet<String>(),
            args[4]->as<ASTLiteral &>().value.safeGet<String>());
    connection = std::make_shared<pqxx::connection>(connection_str);
}


void registerTableFunctionPostgreSQL(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionPostgreSQL>();
}

}

#endif
