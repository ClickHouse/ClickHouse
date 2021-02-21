#include "ColumnInfoHandler.h"

#if USE_ODBC

#    include <DataTypes/DataTypeFactory.h>
#    include <DataTypes/DataTypeNullable.h>
#    include <IO/WriteBufferFromHTTPServerResponse.h>
#    include <IO/WriteHelpers.h>
#    include <Parsers/ParserQueryWithOutput.h>
#    include <Parsers/parseQuery.h>
#    include <Poco/Data/ODBC/ODBCException.h>
#    include <Poco/Data/ODBC/SessionImpl.h>
#    include <Poco/Data/ODBC/Utility.h>
#    include <Poco/Net/HTMLForm.h>
#    include <Poco/Net/HTTPServerRequest.h>
#    include <Poco/Net/HTTPServerResponse.h>
#    include <Poco/NumberParser.h>
#    include <common/logger_useful.h>
#    include <Common/quoteString.h>
#    include <ext/scope_guard.h>
#    include "getIdentifierQuote.h"
#    include "validateODBCConnectionString.h"

#    define POCO_SQL_ODBC_CLASS Poco::Data::ODBC

namespace DB
{
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

void ODBCColumnsInfoHandler::handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
{
    Poco::Net::HTMLForm params(request, request.stream());
    LOG_TRACE(log, "Request URI: {}", request.getURI());

    auto process_error = [&response, this](const std::string & message)
    {
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

        POCO_SQL_ODBC_CLASS::SessionImpl session(validateODBCConnectionString(connection_string), DBMS_DEFAULT_CONNECT_TIMEOUT_SEC);
        SQLHDBC hdbc = session.dbc().handle();

        SQLHSTMT hstmt = nullptr;

        if (POCO_SQL_ODBC_CLASS::Utility::isError(SQLAllocStmt(hdbc, &hstmt)))
            throw POCO_SQL_ODBC_CLASS::ODBCException("Could not allocate connection handle.");

        SCOPE_EXIT(SQLFreeStmt(hstmt, SQL_DROP));

        const auto & context_settings = context.getSettingsRef();

        /// TODO Why not do SQLColumns instead?
        std::string name = schema_name.empty() ? backQuoteIfNeed(table_name) : backQuoteIfNeed(schema_name) + "." + backQuoteIfNeed(table_name);
        WriteBufferFromOwnString buf;
        std::string input = "SELECT * FROM " + name + " WHERE 1 = 0";
        ParserQueryWithOutput parser;
        ASTPtr select = parseQuery(parser, input.data(), input.data() + input.size(), "", context_settings.max_query_size, context_settings.max_parser_depth);

        IAST::FormatSettings settings(buf, true);
        settings.always_quote_identifiers = true;
        settings.identifier_quoting_style = getQuotingStyle(hdbc);
        select->format(settings);
        std::string query = buf.str();

        LOG_TRACE(log, "Inferring structure with query '{}'", query);

        if (POCO_SQL_ODBC_CLASS::Utility::isError(POCO_SQL_ODBC_CLASS::SQLPrepare(hstmt, reinterpret_cast<SQLCHAR *>(query.data()), query.size())))
            throw POCO_SQL_ODBC_CLASS::DescriptorException(session.dbc());

        if (POCO_SQL_ODBC_CLASS::Utility::isError(SQLExecute(hstmt)))
            throw POCO_SQL_ODBC_CLASS::StatementException(hstmt);

        SQLSMALLINT cols = 0;
        if (POCO_SQL_ODBC_CLASS::Utility::isError(SQLNumResultCols(hstmt, &cols)))
            throw POCO_SQL_ODBC_CLASS::StatementException(hstmt);

        /// TODO cols not checked

        NamesAndTypesList columns;
        for (SQLSMALLINT ncol = 1; ncol <= cols; ++ncol)
        {
            SQLSMALLINT type = 0;
            /// TODO Why 301?
            SQLCHAR column_name[301];

            SQLSMALLINT is_nullable;
            const auto result = POCO_SQL_ODBC_CLASS::SQLDescribeCol(hstmt, ncol, column_name, sizeof(column_name), nullptr, &type, nullptr, nullptr, &is_nullable);
            if (POCO_SQL_ODBC_CLASS::Utility::isError(result))
                throw POCO_SQL_ODBC_CLASS::StatementException(hstmt);

            auto column_type = getDataType(type);
            if (external_table_functions_use_nulls && is_nullable == SQL_NULLABLE)
            {
                column_type = std::make_shared<DataTypeNullable>(column_type);
            }

            columns.emplace_back(reinterpret_cast<char *>(column_name), std::move(column_type));
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
