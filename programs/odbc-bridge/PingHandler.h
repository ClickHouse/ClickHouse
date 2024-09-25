#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>

namespace DB
{

/// Simple ping handler, answers "Ok." to GET request
class PingHandler : public HTTPRequestHandler
{
public:
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
};

}
