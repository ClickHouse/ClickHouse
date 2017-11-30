#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionODBC.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/StorageODBC.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>

#include <Poco/Data/ODBC/SessionImpl.h>
#include <Poco/Data/ODBC/ODBCException.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

IDataType* getDataType(SQLSMALLINT type)
{
    switch (type)
    {
        case SQL_NUMERIC:
        case SQL_DECIMAL:
        case SQL_INTEGER:
            return new DataTypeUInt32;
        case SQL_SMALLINT:
            return new DataTypeUInt16;
        case SQL_FLOAT:
        case SQL_REAL:
            return new DataTypeFloat32;
        case SQL_DOUBLE:
            return new DataTypeFloat64;
        case SQL_DATETIME:
        case SQL_TYPE_TIMESTAMP:
            return new DataTypeDateTime;
        case SQL_TYPE_DATE:
            return new DataTypeDate;
        default:
            return new DataTypeString;
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
        throw Exception("Table function 'odbc' requires exactly 2 arguments: database name and table name.",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (int i = 0; i < 2; i++)
        args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(args[i], context);

    std::string database_name = static_cast<const ASTLiteral &>(*args[0]).value.safeGet<String>();
    std::string table_name = static_cast<const ASTLiteral &>(*args[1]).value.safeGet<String>();
    Poco::Data::ODBC::SessionImpl session(database_name, 60);
    SQLHDBC hdbc = session.dbc().handle();
    SQLHSTMT hstmt;
    if (SQLAllocStmt(hdbc, &hstmt) != SQL_SUCCESS)
    {
        throw Poco::Data::ODBC::DescriptorException(session.dbc());
    }
    try {
        std::string query = "SELECT * FROM " + table_name + " WHERE 1=0";
        if (SQLPrepare(hstmt, (SQLCHAR*)query.c_str(), query.size()) != SQL_SUCCESS )
        {
            throw Poco::Data::ODBC::DescriptorException(session.dbc());
        }
        SQLINTEGER ret = SQLExecute(hstmt);

        if ((ret != SQL_SUCCESS) && (ret != SQL_SUCCESS_WITH_INFO))
        {
            throw Poco::Data::ODBC::DescriptorException(session.dbc());
        }

        SQLSMALLINT cols;
        if (SQLNumResultCols(hstmt, &cols) != SQL_SUCCESS)
        {
            throw Poco::Data::ODBC::ConnectionException(session.dbc());
        }
//        fprintf(stderr, "Column count: %u\n", cols);
        NamesAndTypesListPtr columns = std::make_shared<NamesAndTypesList>();
        NamesAndTypesList materialized_columns;
        NamesAndTypesList alias_columns;
        ColumnDefaults column_defaults;
        for (int ncol = 1; ncol <= cols; ncol++)
        {
            SQLSMALLINT type = 0;
            SQLCHAR column_name[301];
            SQLDescribeCol(hstmt, ncol, column_name, sizeof(column_name), NULL, &type, NULL, NULL, NULL);
            columns->push_back(NameAndTypePair((char*)column_name, std::shared_ptr<IDataType>(getDataType(type))));
//            fprintf(stderr, "Column name: %s type: %i\n", column_name, type);
        }
        auto result = StorageODBC::create(table_name, database_name, table_name, columns, materialized_columns, alias_columns, column_defaults, context);
        result->startup();
        return result;
    }
    catch (...) {
        SQLFreeStmt(hstmt, SQL_DROP);
        throw;
    }
    return nullptr;
}


void registerTableFunctionODBC(TableFunctionFactory & factory)
{
    TableFunctionFactory::instance().registerFunction<TableFunctionODBC>();
}

}
