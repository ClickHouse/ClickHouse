#pragma once

#include <Core/Names.h>
#include <Common/CurrentThread.h>
#include <Common/setThreadName.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/HTTPRequestHandler.h>
#include <Server/WebSocket/WebSocket.h>
#include <Server/WebSocket/WebSocketServerConnection.h>
#include <Server/WebSocket/ReadBufferFromWebSocket.h>

#include "Poco/Util/ServerApplication.h"
#include <Poco/Net/WebSocket.h>


namespace DB
{

class Session;
class Credentials;
class IServer;

class HTTPWebSocketHandler : public HTTPRequestHandler
{
private:
    IServer & server;
    Poco::Logger * log;

    std::unique_ptr<Session> session;

    /// Reference to the immutable settings in the global context.
    /// Those settings are used only to extract a http request's parameters.
    /// See settings http_max_fields, http_max_field_name_size, http_max_field_value_size in HTMLForm.
    const Settings & default_settings;

    // The request_credential instance may outlive a single request/response loop.
    // This happens only when the authentication mechanism requires more than a single request/response exchange (e.g., SPNEGO).
    std::unique_ptr<Credentials> request_credentials;

    bool authenticateUser(
        HTTPServerRequest & request,
        HTMLForm & params,
        HTTPServerResponse & response);

public:
    HTTPWebSocketHandler(IServer & server_);
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;

    virtual bool customizeQueryParam(ContextMutablePtr context, const std::string & key, const std::string & value) = 0;

    virtual std::string getQuery(HTTPServerRequest & request, HTMLForm & params, ContextMutablePtr context) = 0;

    virtual void customizeContext(HTTPServerRequest & /* request */, ContextMutablePtr /* context */, ReadBuffer & /* body */) {}
};


class DynamicQueryWebSocketHandler : public HTTPWebSocketHandler
{
private:
    std::string param_name;
public:
    explicit DynamicQueryWebSocketHandler(IServer & server_, const std::string & param_name_);

    std::string getQuery(HTTPServerRequest & request, HTMLForm & params, ContextMutablePtr context) override;

    bool customizeQueryParam(ContextMutablePtr context, const std::string &key, const std::string &value) override;
};
}
