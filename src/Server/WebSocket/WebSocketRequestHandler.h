#pragma once

#include <Common/CurrentThread.h>
#include <Server/WebSocket/WebSocket.h>
#include <Server/WebSocket/WebSocketRequestHandler.h>
#include <Server/HTTP/HTMLForm.h>
#include <Interpreters/Session.h>
#include <Server/IServer.h>
#include <Server/WebSocket/WriteBufferFromWebSocket.h>
#include <Interpreters/executeQuery.h>

#include <IO/ReadBufferFromString.h>


#include <Poco/JSON/Object.h>
#include <boost/noncopyable.hpp>

namespace DB
{

class WebSocketRequestHandler
{
public:
    explicit WebSocketRequestHandler(IServer& /*iServer*/)
//        : server(iServer)
    {
    }

    void handleRequest(Poco::JSON::Object & request, WebSocket & webSocket);
private:
//    IServer& server;
    std::unique_ptr<Session> session;

    void processQuery(
        Poco::JSON::Object & request,
        WriteBuffer & output,
        std::optional<CurrentThread::QueryScope> & query_scope
    );
};
}
