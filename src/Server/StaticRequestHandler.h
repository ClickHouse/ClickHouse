#pragma once

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
    String content_type;
    String response_expression;

public:
    StaticRequestHandler(
        IServer & server,
        const String & expression,
        int status_ = 200,
        const String & content_type_ = "text/html; charset=UTF-8");

    void writeResponse(WriteBuffer & out);

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;
};

}
