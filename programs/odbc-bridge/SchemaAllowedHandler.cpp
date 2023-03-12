#include "SchemaAllowedHandler.h"

#if USE_ODBC

#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Common/BridgeProtocolVersion.h>
#include <Common/logger_useful.h>
#include "validateODBCConnectionString.h"
#include "ODBCPooledConnectionFactory.h"
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
        LOG_WARNING(log, fmt::runtime(message));
    };

    size_t version;

    if (!params.has("version"))
        version = 0; /// assumed version for too old servers which do not send a version
    else
    {
        String version_str = params.get("version");
        if (!tryParse(version, version_str))
        {
            process_error("Unable to parse 'version' string in request URL: '" + version_str + "' Check if the server and library-bridge have the same version.");
            return;
        }
    }

    if (version != XDBC_BRIDGE_PROTOCOL_VERSION)
    {
        /// backwards compatibility is considered unnecessary for now, just let the user know that the server and the bridge must be upgraded together
        process_error("Server and library-bridge have different versions: '" + std::to_string(version) + "' vs. '" + std::to_string(LIBRARY_BRIDGE_PROTOCOL_VERSION) + "'");
        return;
    }


    if (!params.has("connection_string"))
    {
        process_error("No 'connection_string' in request URL");
        return;
    }

    try
    {
        std::string connection_string = params.get("connection_string");

        auto connection = ODBCPooledConnectionFactory::instance().get(
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
