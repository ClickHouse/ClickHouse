#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>


namespace DB
{

class IServer;

/// Response with HTML page that allows to send queries and show results in browser.
class WebUIRequestHandler : public HTTPRequestHandler
{
private:
    IServer & server;

public:
    WebUIRequestHandler(IServer & server_);
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;
};

}
