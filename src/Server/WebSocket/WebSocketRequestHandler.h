#pragma once

#include <Common/CurrentThread.h>
#include <Server/WebSocket/WebSocket.h>
#include <Server/WebSocket/WebSocketRequestHandler.h>
#include <Server/HTTP/HTMLForm.h>

#include <Poco/JSON/Object.h>
#include <boost/noncopyable.hpp>

namespace DB
{

class WebSocketRequestHandler : public WebSocketRequestHandler
{
public:
    void handleRequest(Poco::JSON::Object & request, WebSocket & webSocket) override;
private:
    void processQuery(
        HTTPServerRequest & request,
        HTMLForm & params,
        WriteBuffer & simple_output,
        ReadBuffer & web_socket_input,
        std::optional<CurrentThread::QueryScope> & query_scope
    );
};
}
