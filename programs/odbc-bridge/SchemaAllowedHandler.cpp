#include "SchemaAllowedHandler.h"

#if USE_ODBC

#    include <Server/HTTP/HTMLForm.h>
#    include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#    include <IO/WriteHelpers.h>
#    include <Poco/Data/ODBC/ODBCException.h>
#    include <Poco/Data/ODBC/SessionImpl.h>
#    include <Poco/Data/ODBC/Utility.h>
#    include <Poco/Net/HTTPServerRequest.h>
#    include <Poco/Net/HTTPServerResponse.h>
#    include <common/logger_useful.h>
#    include "validateODBCConnectionString.h"

#    define POCO_SQL_ODBC_CLASS Poco::Data::ODBC

namespace DB
{
namespace
{
    bool isSchemaAllowed(SQLHDBC hdbc)
    {
        SQLUINTEGER value;
        SQLSMALLINT value_length = sizeof(value);
        SQLRETURN r = POCO_SQL_ODBC_CLASS::SQLGetInfo(hdbc, SQL_SCHEMA_USAGE, &value, sizeof(value), &value_length);

        if (POCO_SQL_ODBC_CLASS::Utility::isError(r))
            throw POCO_SQL_ODBC_CLASS::ConnectionException(hdbc);

        return value != 0;
    }
}


void SchemaAllowedHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response)
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

        bool result = isSchemaAllowed(hdbc);

        WriteBufferFromHTTPServerResponse out(response, request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD, keep_alive_timeout);
        try
        {
            writeBoolText(result, out);
            out.finalize();
        }
        catch (...)
        {
            out.finalize();
        }
    }
    catch (...)
    {
        process_error("Error getting schema usage from ODBC '" + getCurrentExceptionMessage(false) + "'");
        tryLogCurrentException(log);
    }
}

}

#endif
