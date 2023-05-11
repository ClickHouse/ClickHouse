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
        WebSocketServerConnection(IServer& iServer, WebSocket& socket, std::shared_ptr<Session>& session_) :
            server(iServer),
            webSocket(socket),
            frame_buffer(0),
            message_buffer(0),
            control_frames_handler(),
            regular_handler(server, session_)
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
