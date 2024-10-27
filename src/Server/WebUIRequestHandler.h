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
    explicit PlayWebUIRequestHandler(IServer & server_);
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
};

class DashboardWebUIRequestHandler : public HTTPRequestHandler
{
private:
    IServer & server;
public:
    explicit DashboardWebUIRequestHandler(IServer & server_);
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
};

class BinaryWebUIRequestHandler : public HTTPRequestHandler
{
private:
    IServer & server;
public:
    explicit BinaryWebUIRequestHandler(IServer & server_);
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
};

class MergesWebUIRequestHandler : public HTTPRequestHandler
{
private:
    IServer & server;
public:
    explicit MergesWebUIRequestHandler(IServer & server_);
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
};

class JavaScriptWebUIRequestHandler : public HTTPRequestHandler
{
private:
    IServer & server;
public:
    explicit JavaScriptWebUIRequestHandler(IServer & server_);
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
};

}
