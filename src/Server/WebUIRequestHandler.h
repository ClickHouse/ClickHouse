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
    std::string resource_name;
public:
    WebUIRequestHandler(IServer & server_, std::string resource_name_);
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;
};

}

