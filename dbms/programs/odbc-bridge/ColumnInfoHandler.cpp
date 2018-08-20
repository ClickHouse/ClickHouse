#include "ColumnInfoHandler.h"
#if USE_POCO_SQLODBC || USE_POCO_DATAODBC
#include <Poco/Data/ODBC/ODBCException.h>
#include <Poco/Data/ODBC/SessionImpl.h>
#include <Poco/Data/ODBC/Utility.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>

#include <DataTypes/DataTypeFactory.h>
#include <IO/WriteBufferFromHTTPServerResponse.h>
#include <IO/WriteHelpers.h>
#include <Common/HTMLForm.h>
#include <common/logger_useful.h>
#include <ext/scope_guard.h>
#include "validateODBCConnectionString.h"

namespace DB
{
namespace
{
    DataTypePtr getDataType(SQLSMALLINT type)
    {
        const auto & factory = DataTypeFactory::instance();

        switch (type)
        {
            case SQL_INTEGER:
                return factory.get("Int32");
            case SQL_SMALLINT:
                return factory.get("Int16");
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
}

void ODBCColumnsInfoHandler::handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
{
    Poco::Net::HTMLForm params(request, request.stream());
    LOG_TRACE(log, "Request URI: " + request.getURI());

    auto process_error = [&response, this](const std::string & message) {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
        if (!response.sent())
            response.send() << message << std::endl;
        LOG_WARNING(log, message);
    };

    if (!params.has("table"))
    {
        process_error("No 'table' param in request URL");
        return;
    }
    if (!params.has("connection_string"))
    {
        process_error("No 'connection_string' in request URL");
        return;
    }
    std::string table_name = params.get("table");
    std::string connection_string = params.get("connection_string");
    LOG_TRACE(log, "Will fetch info for table '" << table_name << "'");
    LOG_TRACE(log, "Got connection str '" << connection_string << "'");

    try
    {
        Poco::Data::ODBC::SessionImpl session(validateODBCConnectionString(connection_string), DBMS_DEFAULT_CONNECT_TIMEOUT_SEC);
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

        WriteBufferFromHTTPServerResponse out(request, response, keep_alive_timeout);
        writeStringBinary(columns.toString(), out);
    }
    catch (...)
    {
        process_error("Error getting columns from ODBC '" + getCurrentExceptionMessage(false) + "'");
        tryLogCurrentException(log);
    }
}
}
#endif
