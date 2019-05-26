#include "IdentifierQuoteHandler.h"
#if USE_POCO_SQLODBC || USE_POCO_DATAODBC

#if USE_POCO_SQLODBC
#include <Poco/SQL/ODBC/ODBCException.h> // Y_IGNORE
#include <Poco/SQL/ODBC/SessionImpl.h> // Y_IGNORE
#include <Poco/SQL/ODBC/Utility.h> // Y_IGNORE
#define POCO_SQL_ODBC_CLASS Poco::SQL::ODBC
#endif
#if USE_POCO_DATAODBC
#include <Poco/Data/ODBC/ODBCException.h>
#include <Poco/Data/ODBC/SessionImpl.h>
#include <Poco/Data/ODBC/Utility.h>
#define POCO_SQL_ODBC_CLASS Poco::Data::ODBC
#endif

#include <DataTypes/DataTypeFactory.h>
#include <IO/WriteBufferFromHTTPServerResponse.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/parseQuery.h>
#include <Poco/Net/HTMLForm.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <common/logger_useful.h>
#include <ext/scope_guard.h>
#include "getIdentifierQuote.h"
#include "validateODBCConnectionString.h"

namespace DB
{
void IdentifierQuoteHandler::handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
{
    Poco::Net::HTMLForm params(request, request.stream());
    LOG_TRACE(log, "Request URI: " + request.getURI());

    auto process_error = [&response, this](const std::string & message)
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
        if (!response.sent())
            response.send() << message << std::endl;
        LOG_WARNING(log, message);
    };

    if (!params.has("connection_string"))
    {
        process_error("No 'connection_string' in request URL");
        return;
    }

    try
    {
        std::string connection_string = params.get("connection_string");
        POCO_SQL_ODBC_CLASS::SessionImpl session(validateODBCConnectionString(connection_string), DBMS_DEFAULT_CONNECT_TIMEOUT_SEC);
        SQLHDBC hdbc = session.dbc().handle();

        auto identifier = getIdentifierQuote(hdbc);

        WriteBufferFromHTTPServerResponse out(request, response, keep_alive_timeout);
        writeStringBinary(identifier, out);
    }
    catch (...)
    {
        process_error("Error getting identifier quote style from ODBC '" + getCurrentExceptionMessage(false) + "'");
        tryLogCurrentException(log);
    }
}
}
#endif
