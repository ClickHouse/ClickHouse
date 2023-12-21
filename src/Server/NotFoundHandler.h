#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>

namespace DB
{

/// Response with 404 and verbose description.
class NotFoundHandler : public HTTPRequestHandler
{
public:
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;
};

}
