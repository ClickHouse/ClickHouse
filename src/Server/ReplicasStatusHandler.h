#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>

namespace DB
{

class Context;
class IServer;

/// Replies "Ok.\n" if all replicas on this server don't lag too much. Otherwise output lag information.
class ReplicasStatusHandler : public HTTPRequestHandler, WithContext
{
public:
    explicit ReplicasStatusHandler(IServer & server_);

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;
};


}
