#include <TableFunctions/TableFunctionODBC.h>

#if Poco_DataODBC_FOUND
#include <type_traits>
#include <ext/scope_guard.h>

#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageODBC.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Core/Defines.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wsign-compare"
    #include <Poco/Data/ODBC/ODBCException.h>
    #include <Poco/Data/ODBC/SessionImpl.h>
    #include <Poco/Data/ODBC/Utility.h>
#pragma GCC diagnostic pop


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

DataTypePtr getDataType(SQLSMALLINT type)
{
    const auto & factory = DataTypeFactory::instance();

    switch (type)
    {
        case SQL_INTEGER:
            return factory.get("UInt32");
        case SQL_SMALLINT:
            return factory.get("UInt16");
        case SQL_FLOAT:
            return factory.get("Float32");
        case SQL_REAL:
            return factory.get("Float32");
        case SQL_DOUBLE:
            return factory.get("Float64");
        case SQL_DATETIME:
            return factory.get("DateTime");
        case SQL_TYPE_TIMESTAMP:
            return factory.get("DateTime");
        case SQL_TYPE_DATE:
            return factory.get("Date");
        default:
            return factory.get("String");
    }
}

StoragePtr TableFunctionODBC::execute(const ASTPtr & ast_function, const Context & context) const
{
    const ASTFunction & args_func = typeid_cast<const ASTFunction &>(*ast_function);

    if (!args_func.arguments)
        throw Exception("Table function 'odbc' must have arguments.", ErrorCodes::LOGICAL_ERROR);

    ASTs & args = typeid_cast<ASTExpressionList &>(*args_func.arguments).children;

    if (args.size() != 2)
        throw Exception("Table function 'odbc' requires exactly 2 arguments: ODBC connection string and table name.",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (int i = 0; i < 2; ++i)
        args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(args[i], context);

    std::string connection_string = static_cast<const ASTLiteral &>(*args[0]).value.safeGet<String>();
    std::string table_name = static_cast<const ASTLiteral &>(*args[1]).value.safeGet<String>();

    Poco::Data::ODBC::SessionImpl session(connection_string, DBMS_DEFAULT_CONNECT_TIMEOUT_SEC);
    SQLHDBC hdbc = session.dbc().handle();

    SQLHSTMT hstmt = nullptr;

    if (Poco::Data::ODBC::Utility::isError(SQLAllocStmt(hdbc, &hstmt)))
        throw Poco::Data::ODBC::ODBCException("Could not allocate connection handle.");

    SCOPE_EXIT(SQLFreeStmt(hstmt, SQL_DROP));

    /// TODO Why not do SQLColumns instead?
    std::string query = "SELECT * FROM " + table_name + " WHERE 1 = 0";
    if (Poco::Data::ODBC::Utility::isError(Poco::Data::ODBC::SQLPrepare(hstmt, reinterpret_cast<SQLCHAR *>(&query[0]), query.size())))
        throw Poco::Data::ODBC::DescriptorException(session.dbc());

    if (Poco::Data::ODBC::Utility::isError(SQLExecute(hstmt)))
        throw Poco::Data::ODBC::StatementException(hstmt);

    SQLSMALLINT cols = 0;
    if (Poco::Data::ODBC::Utility::isError(SQLNumResultCols(hstmt, &cols)))
        throw Poco::Data::ODBC::StatementException(hstmt);

    /// TODO cols not checked

    NamesAndTypesList columns;
    for (SQLSMALLINT ncol = 1; ncol <= cols; ++ncol)
    {
        SQLSMALLINT type = 0;
        /// TODO Why 301?
        SQLCHAR column_name[301];
        /// TODO Result is not checked.
        Poco::Data::ODBC::SQLDescribeCol(hstmt, ncol, column_name, sizeof(column_name), NULL, &type, NULL, NULL, NULL);
        columns.emplace_back(reinterpret_cast<char *>(column_name), getDataType(type));
    }

    auto result = StorageODBC::create(table_name, connection_string, "", table_name, columns);
    result->startup();
    return result;
}


void registerTableFunctionODBC(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionODBC>();
}
}

#endif
