#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>


namespace DB
{

class IServer;

/// Response with HTML page that allows to send queries and show results in browser.

class PlayWebUIRequestHandler : public HTTPRequestHandler
{
private:
    IServer & server;
public:
    PlayWebUIRequestHandler(IServer & server_);
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
};

class DashboardWebUIRequestHandler : public HTTPRequestHandler
{
private:
    IServer & server;
public:
    DashboardWebUIRequestHandler(IServer & server_);
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
};

class BinaryWebUIRequestHandler : public HTTPRequestHandler
{
private:
    IServer & server;
public:
    BinaryWebUIRequestHandler(IServer & server_);
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
};

class JavaScriptWebUIRequestHandler : public HTTPRequestHandler
{
private:
    IServer & server;
public:
    JavaScriptWebUIRequestHandler(IServer & server_);
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
};

}
