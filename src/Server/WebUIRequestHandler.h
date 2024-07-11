#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>


namespace DB
{

class IServer;

/// Response with HTML page that allows to send queries and show results in browser.

class PlayWebUIRequestHandler : public HTTPRequestHandler
{
public:
    explicit PlayWebUIRequestHandler(IServer &) {}
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
};

class DashboardWebUIRequestHandler : public HTTPRequestHandler
{
public:
    explicit DashboardWebUIRequestHandler(IServer &) {}
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
};

class BinaryWebUIRequestHandler : public HTTPRequestHandler
{
public:
    explicit BinaryWebUIRequestHandler(IServer &) {}
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
};

class MergesWebUIRequestHandler : public HTTPRequestHandler
{
public:
    explicit MergesWebUIRequestHandler(IServer &) {}
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
};

class ACMERequestHandler : public HTTPRequestHandler
{
private:
    IServer & server;
public:
    explicit ACMERequestHandler(IServer & server_);
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
};

class JavaScriptWebUIRequestHandler : public HTTPRequestHandler
{
public:
    explicit JavaScriptWebUIRequestHandler(IServer &) {}
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
};

}
