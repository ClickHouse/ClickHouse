#pragma once

#include <condition_variable>
#include <mutex>
#include "config.h"
#if USE_NGHTTP2

#include <Common/ProfileEvents.h>

#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>

#include <Server/HTTP/HTTP2/HTTP2ServerParams.h>
#include <Server/HTTP/HTTP2/HTTP2Stream.h>
#include <Server/HTTP/HTTPContext.h>
#include <Server/HTTP/HTTPRequestHandlerFactory.h>
#include <Server/TCPServer.h>

#include <Poco/Net/TCPServerConnection.h>
#include <Poco/Pipe.h>

#include <poll.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdocumentation"
    #include <nghttp2/nghttp2.h>
#pragma clang diagnostic pop

namespace DB
{

bool isHTTP2Connection(const Poco::Net::StreamSocket & socket, HTTP2ServerParams::Ptr http2_params);

class HTTP2ServerConnection : public Poco::Net::TCPServerConnection
{
public:
    HTTP2ServerConnection(
        HTTPContextPtr context,
        TCPServer & tcp_server,
        const Poco::Net::StreamSocket & socket,
        HTTP2ServerParams::Ptr params,
        HTTPRequestHandlerFactoryPtr factory,
        Poco::ThreadPool & thread_pool,
        const ProfileEvents::Event & read_event_ = ProfileEvents::end(),
        const ProfileEvents::Event & write_event_ = ProfileEvents::end());

    HTTP2ServerConnection(
        HTTPContextPtr context_,
        TCPServer & tcp_server_,
        const Poco::Net::StreamSocket & socket_,
        HTTP2ServerParams::Ptr params_,
        HTTPRequestHandlerFactoryPtr factory_,
        Poco::ThreadPool & thread_pool_,
        const String & forwarded_for_,
        const ProfileEvents::Event & read_event_ = ProfileEvents::end(),
        const ProfileEvents::Event & write_event_ = ProfileEvents::end())
    : HTTP2ServerConnection(context_, tcp_server_, socket_, params_, factory_, thread_pool_, read_event_, write_event_)
    {
        forwarded_for = forwarded_for_;
    }

    void run() override;

private:
    void doRun();

    void sendPendingFrames();

    bool processNextFrame();
    bool processNextStreamEvent();

    void submit100Continue(uint32_t stream_id);
    void onOutputReady(uint32_t stream_id);
    void prepareHeaders(HTTP2Stream & stream, std::vector<nghttp2_nv> & nva);

    void initSession();

    static ssize_t dataSourceReadCallback(nghttp2_session * session, int32_t stream_id,
        uint8_t * buf, size_t length, uint32_t * data_flags,
        nghttp2_data_source * source, void * user_data);

    static int sendDataCallback(nghttp2_session * session, nghttp2_frame * frame,
        const uint8_t * framehd, size_t length,
        nghttp2_data_source * source, void * user_data);

    static int onBeginHeadersCallback(nghttp2_session * session,
        const nghttp2_frame * frame, void * user_data);

    static int onHeaderCallback(nghttp2_session * session, const nghttp2_frame * frame,
        const uint8_t * name, size_t namelen, const uint8_t * value,
        size_t valuelen, uint8_t flags, void * user_data);

    static int onFrameRecvCallback(nghttp2_session * session,
        const nghttp2_frame * frame, void * user_data);

    static int onStreamCloseCallback(nghttp2_session * session, int32_t stream_id,
        uint32_t error_code, void * user_data);

    static int onDataChunkRecvCallback(nghttp2_session * session, uint8_t flags,
        int32_t stream_id, const uint8_t * data,
        size_t len, void * user_data);

    void notifyStreamClose(HTTP2Stream & stream);

    HTTPContextPtr context;
    TCPServer & tcp_server;
    HTTP2ServerParams::Ptr params;
    HTTPRequestHandlerFactoryPtr factory;
    Poco::ThreadPool & thread_pool;
    String forwarded_for;   /// FIXME: looks ugly
    ProfileEvents::Event read_event;
    ProfileEvents::Event write_event;

    LoggerPtr log;
    std::string peer_address;

    nghttp2_session * session = nullptr;
    nghttp2_option * option_no_auto_window_update = nullptr;
    std::unordered_map<int32_t, std::shared_ptr<HTTP2Stream>> streams;
    size_t active_streams = 0;
    std::mutex active_streams_mutex;
    std::condition_variable active_streams_cv;

    std::unique_ptr<ReadBufferFromPocoSocket> socket_in;
    Memory<> buf;

    std::unique_ptr<WriteBufferFromPocoSocket> socket_out;

    std::shared_ptr<Poco::Pipe> stream_event_pipe;
    std::unique_ptr<ReadBufferFromFileDescriptor> stream_event_pipe_in;

    struct pollfd pfds[2];
};

}

#endif
