#include "ColumnInfoHandler.h"

#if USE_ODBC

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/parseQuery.h>
#include <Server/HTTP/HTMLForm.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/NumberParser.h>
#include <common/logger_useful.h>
#include <Common/quoteString.h>
#include <ext/scope_guard.h>
#include "getIdentifierQuote.h"
#include "validateODBCConnectionString.h"

#include <nanodbc/nanodbc.h>
#include <sql.h>
#include <sqlext.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    DataTypePtr getDataType(SQLSMALLINT type)
    {
        const auto & factory = DataTypeFactory::instance();

        switch (type)
        {
            case SQL_TINYINT:
                return factory.get("Int8");
            case SQL_INTEGER:
                return factory.get("Int32");
            case SQL_SMALLINT:
                return factory.get("Int16");
            case SQL_BIGINT:
                return factory.get("Int64");
            case SQL_FLOAT:
                return factory.get("Float64");
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


void ODBCColumnsInfoHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response)
{
    HTMLForm params(request, request.getStream());
    LOG_TRACE(log, "Request URI: {}", request.getURI());

    auto process_error = [&response, this](const std::string & message)
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
        if (!response.sent())
            *response.send() << message << std::endl;
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
    std::string schema_name;
    std::string table_name = params.get("table");
    std::string connection_string = params.get("connection_string");

    if (params.has("schema"))
    {
        schema_name = params.get("schema");
        LOG_TRACE(log, "Will fetch info for table '{}'", schema_name + "." + table_name);
    }
    else
        LOG_TRACE(log, "Will fetch info for table '{}'", table_name);

    LOG_TRACE(log, "Got connection str '{}'", connection_string);

    try
    {
        const bool external_table_functions_use_nulls = Poco::NumberParser::parseBool(params.get("external_table_functions_use_nulls", "false"));

        nanodbc::connection connection(validateODBCConnectionString(connection_string));
        nanodbc::catalog catalog(connection);
        nanodbc::catalog::columns columns_definition = catalog.find_columns(NANODBC_TEXT("%"), table_name, schema_name);

        NamesAndTypesList columns;
        while (columns_definition.next())
        {
            SQLSMALLINT type = columns_definition.sql_data_type();
            std::string column_name = columns_definition.column_name();

            bool is_nullable = columns_definition.nullable() == SQL_NULLABLE;

            auto column_type = getDataType(type);

            if (external_table_functions_use_nulls && is_nullable == SQL_NULLABLE)
                column_type = std::make_shared<DataTypeNullable>(column_type);

            columns.emplace_back(column_name, std::move(column_type));
        }

        if (columns.empty())
            throw Exception("Columns definition was not returned", ErrorCodes::LOGICAL_ERROR);

        WriteBufferFromHTTPServerResponse out(response, request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD, keep_alive_timeout);
        try
        {
            writeStringBinary(columns.toString(), out);
            out.finalize();
        }
        catch (...)
        {
            out.finalize();
        }
    }
    catch (...)
    {
        process_error("Error getting columns from ODBC '" + getCurrentExceptionMessage(false) + "'");
        tryLogCurrentException(log);
    }
}

}

#endif
