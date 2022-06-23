#include "SchemaAllowedHandler.h"

#if USE_ODBC

#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <IO/WriteHelpers.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <common/logger_useful.h>
#include "validateODBCConnectionString.h"
#include "ODBCConnectionFactory.h"
#include <sql.h>
#include <sqlext.h>


namespace DB
{
namespace
{
    bool isSchemaAllowed(nanodbc::ConnectionHolderPtr connection_holder)
    {
        uint32_t result = execute<uint32_t>(connection_holder,
                    [&](nanodbc::connection & connection) { return connection.get_info<uint32_t>(SQL_SCHEMA_USAGE); });
        return result != 0;
    }
}


void SchemaAllowedHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response)
{
    HTMLForm params(getContext()->getSettingsRef(), request, request.getStream());
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

        auto connection = ODBCConnectionFactory::instance().get(
                validateODBCConnectionString(connection_string),
                getContext()->getSettingsRef().odbc_bridge_connection_pool_size);

        bool result = isSchemaAllowed(std::move(connection));

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
