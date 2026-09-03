#pragma once

#include <unordered_map>
#include <Server/HTTP/HTTPRequestHandler.h>
#include <base/types.h>

namespace DB
{

class IServer;
class WriteBuffer;

/// Response with custom string. Can be used for browser.
class StaticRequestHandler : public HTTPRequestHandler
{
private:
    IServer & server;

    int status;
    /// Overrides for response headers.
    const std::unordered_map<String, String> http_response_headers_override;
    String response_expression;

    void writeResponse(WriteBuffer & out);

public:
    StaticRequestHandler(
        IServer & server,
        const String & expression,
        const std::unordered_map<String, String> & http_response_headers_override_,
        int status_ = 200);

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
};

}
