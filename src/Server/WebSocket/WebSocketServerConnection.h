#include <Server/WebSocket/WebSocket.h>
#include <Server/WebSocket/WebSocketRequestHandler.h>
#include <Server/WebSocket/WebSocketControlFramesHandler.h>

#include <Poco/Runnable.h>


namespace DB
{
class WebSocketServerConnection : public Poco::Runnable
{
    public:
        WebSocketServerConnection(WebSocket& socket) :
            webSocket(socket),
            frame_buffer(DBMS_DEFAULT_BUFFER_SIZE),
            message_buffer(DBMS_DEFAULT_BUFFER_SIZE)
        {
        }

        void run() override;

    private:
        WebSocket& webSocket;
        Poco::Buffer<char> frame_buffer;
        Poco::Buffer<char> message_buffer;
        WebSocketControlFramesHandler& control_frames_handler;
        WebSocketRequestHandler& regular_handler;

        WebSocket& getSocket();
};
}
