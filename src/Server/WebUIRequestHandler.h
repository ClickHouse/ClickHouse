#pragma once

#include "config.h"

#include <Server/HTTP/HTTPRequestHandler.h>


namespace DB
{

class IServer;

/// Response with HTML page that allows to send queries and show results in browser.

class PlayWebUIRequestHandler : public HTTPRequestHandler
{
public:
    explicit PlayWebUIRequestHandler(IServer &) {}
    explicit PlayWebUIRequestHandler(IServer & server_, const std::unordered_map<String, String> & http_response_headers_override_)
        : PlayWebUIRequestHandler(server_)
    {
        http_response_headers_override = http_response_headers_override_;
    }
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
private:
    /// Overrides for response headers.
    std::unordered_map<String, String> http_response_headers_override;
};

class DashboardWebUIRequestHandler : public HTTPRequestHandler
{
public:
    explicit DashboardWebUIRequestHandler(IServer &) {}
    explicit DashboardWebUIRequestHandler(IServer & server_, const std::unordered_map<String, String> & http_response_headers_override_)
        : DashboardWebUIRequestHandler(server_)
    {
        http_response_headers_override = http_response_headers_override_;
    }
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
private:
    /// Overrides for response headers.
    std::unordered_map<String, String> http_response_headers_override;
};

class BinaryWebUIRequestHandler : public HTTPRequestHandler
{
public:
    explicit BinaryWebUIRequestHandler(IServer &) {}
    explicit BinaryWebUIRequestHandler(IServer & server_, const std::unordered_map<String, String> & http_response_headers_override_)
        : BinaryWebUIRequestHandler(server_)
    {
        http_response_headers_override = http_response_headers_override_;
    }
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
private:
    /// Overrides for response headers.
    std::unordered_map<String, String> http_response_headers_override;
};

class MergesWebUIRequestHandler : public HTTPRequestHandler
{
public:
    explicit MergesWebUIRequestHandler(IServer &) {}
    explicit MergesWebUIRequestHandler(IServer & server_, const std::unordered_map<String, String> & http_response_headers_override_)
        : MergesWebUIRequestHandler(server_)
    {
        http_response_headers_override = http_response_headers_override_;
    }
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
private:
    /// Overrides for response headers.
    std::unordered_map<String, String> http_response_headers_override;
};

class JavaScriptWebUIRequestHandler : public HTTPRequestHandler
{
public:
    explicit JavaScriptWebUIRequestHandler(IServer &) {}
    explicit JavaScriptWebUIRequestHandler(IServer & server_, const std::unordered_map<String, String> & http_response_headers_override_)
        : JavaScriptWebUIRequestHandler(server_)
    {
        http_response_headers_override = http_response_headers_override_;
    }
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
private:
    /// Overrides for response headers.
    std::unordered_map<String, String> http_response_headers_override;
};

}
