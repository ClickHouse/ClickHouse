#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>

namespace DB
{

/// Simple ping handler, answers "Ok." to GET request
class PingHandler : public HTTPRequestHandler
{
public:
    explicit PingHandler(size_t keep_alive_timeout_) : keep_alive_timeout(keep_alive_timeout_) {}
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;

private:
    size_t keep_alive_timeout;
};

}
