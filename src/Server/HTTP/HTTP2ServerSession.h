#pragma once

#include <Server/HTTP/HTTP2ServerConnection.h>

#include <Poco/Net/HTTPServerSession.h>

#include <nghttp2/nghttp2.h>

namespace DB {

    class HTTP2ServerStream;

    class HTTP2ServerSession /* : public Poco::Net::HTTPServerSession*/ {
    public:
        HTTP2ServerSession(
            // Poco::Net::HTTPServerParams::Ptr pParams,
            HTTP2ServerConnection* connection_,
            const Poco::Net::StreamSocket & socket_
        );

        ~HTTP2ServerSession();

        int sessionSend();

        int sessionReceive();
        
        bool sessionEnd();

        void sendResponse(HTTP2ServerStream* stream);

        void createStream(int stream_id);

        HTTP2ServerStream* getStream(int stream_id);

        void removeStream(int stream_id);

        HTTP2ServerConnection* connection;

    private:
        int initHTTP2Session();
        
        int sendServerConnectionHeader();

        std::map<int32_t, std::unique_ptr<HTTP2ServerStream>> streams;
        
        Poco::Net::StreamSocket socket;
        Poco::FIFOBuffer input_buffer;

        static constexpr size_t buf_input_size = 8192;

        nghttp2_session* session;
    };

}
