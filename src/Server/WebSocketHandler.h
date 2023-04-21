#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>



namespace DB
{

class IServer;

class WebSocketRequestHandler : public HTTPRequestHandler
{
private:
    IServer & server;

public:
    WebSocketRequestHandler(IServer & server_);
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;
};

}
