#pragma once

#include "config.h"

#if USE_SSL

#include <Server/HTTP/HTTPRequestHandler.h>


namespace DB
{

class IServer;

/// Response for ACME challenges.
class ACMERequestHandler : public HTTPRequestHandler
{

public:
    explicit ACMERequestHandler(IServer &) {}
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
};

}

#endif
