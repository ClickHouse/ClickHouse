#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>


namespace DB
{

class IServer;
class WriteBuffer;

/// Response with the default page, which is "Ok.\n" for scripts and an HTML index page for user agents.
class IndexRequestHandler : public HTTPRequestHandler
{
private:
    IServer & server;
    const std::unordered_map<String, String> http_response_headers_override;

public:
    explicit IndexRequestHandler(IServer & server_) : server(server_) {}
    IndexRequestHandler(IServer & server_, const std::unordered_map<String, String> & http_response_headers_override_);
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
};

}
