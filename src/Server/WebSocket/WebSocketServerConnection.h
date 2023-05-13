#include <Server/IServer.h>
#include <Server/WebSocket/WebSocket.h>
#include <Server/WebSocket/WebSocketRequestHandler.h>
#include <Server/WebSocket/WebSocketControlFramesHandler.h>

#include <Poco/Runnable.h>
#include <Poco/Util/ServerApplication.h>
#include <Poco/JSON/Parser.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_JSON_FORMAT;
    extern const int UNKNOWN_MESSAGE_TYPE;
}

using Poco::Util::Application;

class WebSocketServerConnection : public Poco::Runnable
{
    class Message
    {
        public:
            enum class Types {
                Request,
                Response,
                Metrics,
                Error,
                Unknown,
            };

            static Types getMessageType(const std::string& raw_type)
            {
                try {
                    return type_mapping.at(raw_type);
                } catch(...) {
                    return Types::Unknown;
                }
            }

        private:
            static inline std::unordered_map<std::string, Types> const type_mapping = {
                {"request", Types::Request},
                {"response", Types::Response},
                {"metrics", Types::Metrics},
                {"error", Types::Error},
            };

    };



    public:
        WebSocketServerConnection(
            IServer& iServer,
            WebSocket& socket,
            std::shared_ptr<Session>& session_,
            Poco::Logger &logger
            ) :
            server(iServer),
            webSocket(socket),
            logger_(logger),
            connection_closed(false),
            frame_buffer(0),
            message_buffer(0),
            control_frames_handler(),
            regular_handler(server, session_)
        {
            session_->makeSessionContext();
        }

        void run() override;
        void start();
    private:
        IServer& server;
        WebSocket& webSocket;
        Poco::Logger& logger_;
        bool connection_closed;

        Poco::Buffer<char> frame_buffer;
        Poco::Buffer<char> message_buffer;
        WebSocketControlFramesHandler control_frames_handler;
        WebSocketRequestHandler regular_handler;
        Poco::JSON::Parser parser;


        WebSocket& getSocket();
        Poco::SharedPtr<Poco::JSON::Object> validateRequest(std::string rawRequest);
        void sendErrorMessage(std::string msg);
};
}
