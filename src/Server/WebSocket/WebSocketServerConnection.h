#include <Server/IServer.h>
#include <Server/WebSocket/WebSocket.h>
#include <Server/WebSocket/WebSocketRequestHandler.h>
#include <Server/WebSocket/WebSocketControlFramesHandler.h>

#include <Poco/Runnable.h>
#include <Poco/JSON/Parser.h>


namespace DB
{
class WebSocketServerConnection : public Poco::Runnable
{
    public:
        WebSocketServerConnection(IServer& iServer, WebSocket& socket) :
            server(iServer),
            webSocket(socket),
            frame_buffer(DBMS_DEFAULT_BUFFER_SIZE),
            message_buffer(DBMS_DEFAULT_BUFFER_SIZE),
            regular_handler(server)
        {
        }

        void run() override;

    private:
        IServer& server;
        WebSocket& webSocket;

        Poco::Buffer<char> frame_buffer;
        Poco::Buffer<char> message_buffer;
        WebSocketControlFramesHandler control_frames_handler;
        WebSocketRequestHandler regular_handler;
        Poco::JSON::Parser parser;

        WebSocket& getSocket();
};
}
