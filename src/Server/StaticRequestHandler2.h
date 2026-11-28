#pragma once

#include <unordered_map>
#include <Server/HTTP/HTTP2RequestHandler.h>
#include <base/types.h>
#include "base/defines.h"

namespace DB
{

class IServer;
class WriteBuffer;


class StaticRequestHandler2 : public HTTP2RequestHandler
{
private:
    IServer & server;

    int status;
    /// Overrides for response headers.

public:
    explicit StaticRequestHandler2(
        IServer & server_,
        int status_ = 200)
    : server(server_)
    , status(status_)
    {
        UNUSED(server);
    }

    void handleRequest(HTTP2ServerRequest & request, HTTP2ServerResponse & response) override;
};

}
