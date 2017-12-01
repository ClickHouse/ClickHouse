#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionODBC.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/StorageODBC.h>
#include <DataTypes/DataTypeFactory.h>

#include <Poco/Data/ODBC/SessionImpl.h>
#include <Poco/Data/ODBC/ODBCException.h>
#include "Poco/Data/ODBC/ODBCException.h"
#include <Poco/Data/ODBC/Utility.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

DataTypePtr getDataType(SQLSMALLINT type)
{
    switch (type)
    {
        case SQL_INTEGER:
            return DataTypeFactory::instance().get("UInt32");
        case SQL_SMALLINT:
            return DataTypeFactory::instance().get("UInt16");
        case SQL_FLOAT:
        case SQL_REAL:
            return DataTypeFactory::instance().get("Float32");
        case SQL_DOUBLE:
            return DataTypeFactory::instance().get("Float64");
        case SQL_DATETIME:
        case SQL_TYPE_TIMESTAMP:
            return DataTypeFactory::instance().get("DateTime");
        case SQL_TYPE_DATE:
            return DataTypeFactory::instance().get("Date");
        default:
            return DataTypeFactory::instance().get("String");
    }
}

StoragePtr TableFunctionODBC::execute(const ASTPtr & ast_function, const Context & context) const
{
    ASTs & args_func = typeid_cast<ASTFunction &>(*ast_function).children;

    if (args_func.size() != 1)
        throw Exception("Table function 'numbers' requires exactly one argument: database name and table name.",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    ASTs & args = typeid_cast<ASTExpressionList &>(*args_func.at(0)).children;

    if (args.size() != 2)
        throw Exception("Table function 'odbc' requires exactly 2 arguments: odbc connect sting and table name.",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (int i = 0; i < 2; ++i)
        args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(args[i], context);

    std::string database_name = static_cast<const ASTLiteral &>(*args[0]).value.safeGet<String>();
    std::string table_name = static_cast<const ASTLiteral &>(*args[1]).value.safeGet<String>();
    Poco::Data::ODBC::SessionImpl session(database_name, 60);
    SQLHDBC hdbc = session.dbc().handle();
    SQLHSTMT hstmt;
    if (Poco::Data::ODBC::Utility::isError(SQLAllocStmt(hdbc, &hstmt)))
    {
        throw Poco::Data::ODBC::ODBCException("Could not allocate connection handle.");
    }
    try {
        std::string query = "SELECT * FROM " + table_name + " WHERE 1=0";
        if (Poco::Data::ODBC::Utility::isError(Poco::Data::ODBC::SQLPrepare(hstmt, (SQLCHAR*)query.c_str(), query.size())))
        {
            throw Poco::Data::ODBC::DescriptorException(session.dbc());
        }
        if (Poco::Data::ODBC::Utility::isError(SQLExecute(hstmt)))
        {
            throw Poco::Data::ODBC::StatementException(hstmt);
        }

        SQLSMALLINT cols;
        if (Poco::Data::ODBC::Utility::isError(SQLNumResultCols(hstmt, &cols)))
        {
            throw Poco::Data::ODBC::StatementException(hstmt);
        }
        NamesAndTypesListPtr columns = std::make_shared<NamesAndTypesList>();
        NamesAndTypesList materialized_columns;
        NamesAndTypesList alias_columns;
        ColumnDefaults column_defaults;
        for (int ncol = 1; ncol <= cols; ++ncol)
        {
            SQLSMALLINT type = 0;
            SQLCHAR column_name[301];
            Poco::Data::ODBC::SQLDescribeCol(hstmt, ncol, column_name, sizeof(column_name), NULL, &type, NULL, NULL, NULL);
            columns->push_back(NameAndTypePair((char*)column_name, getDataType(type)));
//            fprintf(stderr, "Column name: %s type: %i\n", column_name, type);
        }
        SQLFreeStmt(hstmt, SQL_DROP);
        auto result = StorageODBC::create(table_name, database_name, table_name, columns, materialized_columns, alias_columns, column_defaults, context);
        result->startup();
        return result;
    }
    catch (...) {
        SQLFreeStmt(hstmt, SQL_DROP);
        throw;
    }
}


void registerTableFunctionODBC(TableFunctionFactory & factory)
{
    TableFunctionFactory::instance().registerFunction<TableFunctionODBC>();
}

}
