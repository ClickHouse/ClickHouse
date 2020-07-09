#include "SchemaAllowedHandler.h"

#if USE_ODBC

#    include <IO/WriteBufferFromHTTPServerResponse.h>
#    include <IO/WriteHelpers.h>
#    include <Poco/Data/ODBC/ODBCException.h>
#    include <Poco/Data/ODBC/SessionImpl.h>
#    include <Poco/Data/ODBC/Utility.h>
#    include <Poco/Net/HTMLForm.h>
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
        std::string identifier;

        SQLSMALLINT t;
        SQLRETURN r = POCO_SQL_ODBC_CLASS::SQLGetInfo(hdbc, SQL_SCHEMA_USAGE, nullptr, 0, &t);

        if (POCO_SQL_ODBC_CLASS::Utility::isError(r))
            throw POCO_SQL_ODBC_CLASS::ConnectionException(hdbc);

        return t != 0;
    }
}


void SchemaAllowedHandler::handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
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

        WriteBufferFromHTTPServerResponse out(request, response, keep_alive_timeout);
        writeBoolText(result, out);
    }
    catch (...)
    {
        process_error("Error getting schema usage from ODBC '" + getCurrentExceptionMessage(false) + "'");
        tryLogCurrentException(log);
    }
}

}

#endif
