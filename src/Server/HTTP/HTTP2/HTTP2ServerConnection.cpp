#include <base/scope_guard.h>

#include <Common/Logger.h>
#include <Common/logger_useful.h>

#include <IO/ReadBufferFromPocoSocket.h>

#include <Server/HTTP/HTTP2/HTTP2ServerConnection.h>
#include <Server/HTTP/HTTP2/HTTP2ServerParams.h>

#include <Poco/Exception.h>
#include <Poco/Format.h>
#include <Poco/Net/SecureServerSocket.h>
#include <Poco/Net/SecureSocketImpl.h>
#include <Poco/Net/SecureStreamSocketImpl.h>
#include <Poco/Random.h>

#include <sys/poll.h>
#include <sys/socket.h>

namespace DB
{

/// RFC 9113 section 4.1.
extern const size_t FRAME_HEADER_SIZE = 9;
namespace
{

/// RFC9113 section 3.4.
const char HTTP2_CLIENT_PREFACE[24] = {'P', 'R', 'I', ' ', '*', ' ', 'H', 'T', 'T', 'P', '/', '2', '.', '0', '\r', '\n', '\r', '\n', 'S', 'M', '\r', '\n', '\r', '\n'};

const std::string HTTP2_ALPN = "h2";

unsigned char ALL_ALPN_PROTOCOLS[] =
{
    0x02, 'h', '2',
    0x08, 'h','t','t','p','/','1','.','1',
    0x08, 'h','t','t','p','/','1','.','0',
};

unsigned char H2_ALPN_PROTOCOLS[] =
{
    0x02, 'h', '2',
};

uint8_t STATUS_PSEUDOHEADER[] = {':', 's', 't', 'a', 't', 'u', 's'};

int alpnSelectCb(
    SSL* /*ssl*/,
    const unsigned char** out,
    unsigned char* outlen,
    const unsigned char* in,
    unsigned int inlen,
    void* /*arg*/)
{
    int res = SSL_select_next_proto(
        const_cast<unsigned char**>(out),
        outlen,
        H2_ALPN_PROTOCOLS,
        sizeof(H2_ALPN_PROTOCOLS),
        in, inlen);
    if (res == OPENSSL_NPN_NEGOTIATED)
        return SSL_TLSEXT_ERR_OK;
    return SSL_TLSEXT_ERR_NOACK;
}

}

bool setHTTP2Alpn(const Poco::Net::SecureServerSocket & socket, HTTP2ServerParams::Ptr http2_params)
{
    if (!http2_params)
        return false;

    Poco::Net::Context::Ptr context = socket.context();
    if (!context)
        return false;

    SSL_CTX * ssl_ctx = context->sslContext();
    if (!ssl_ctx)
        return false;

    SSL_CTX_set_alpn_protos(ssl_ctx, ALL_ALPN_PROTOCOLS, sizeof(ALL_ALPN_PROTOCOLS));
    SSL_CTX_set_alpn_select_cb(ssl_ctx, alpnSelectCb, nullptr);

    return true;
}

bool isHTTP2Connection(const Poco::Net::StreamSocket & socket, HTTP2ServerParams::Ptr http2_params)
{
    if (!http2_params)
        return false;

    if (!socket.secure())
    {
        /// Prior knowledge
        char buf[sizeof(HTTP2_CLIENT_PREFACE)];
        size_t n = const_cast<Poco::Net::StreamSocket &>(socket).receiveBytes(buf, sizeof(buf), MSG_PEEK);
        if (n != sizeof(buf))
            return false;
        return memcmp(buf, HTTP2_CLIENT_PREFACE, sizeof(buf)) == 0;
    }

    /// dynamic_cast looks like a hack but can't think of a better way
    Poco::Net::SecureStreamSocketImpl * ssocket = dynamic_cast<Poco::Net::SecureStreamSocketImpl *>(socket.impl());
    chassert(ssocket != nullptr);  /// If this happens then something has changed in Poco internals
    if (ssocket == nullptr)
        return false;

    ssocket->completeHandshake();
    std::string alpn_selected = ssocket->getAlpnSelected();
    return alpn_selected == HTTP2_ALPN;
}

HTTP2ServerConnection::HTTP2ServerConnection(
    HTTPContextPtr context_,
    TCPServer & tcp_server_,
    const Poco::Net::StreamSocket & socket_,
    HTTP2ServerParams::Ptr params_,
    HTTPRequestHandlerFactoryPtr factory_,
    Poco::ThreadPool & thread_pool_,
    const ProfileEvents::Event & read_event_,
    const ProfileEvents::Event & write_event_)
    : TCPServerConnection(socket_)
    , context(std::move(context_))
    , tcp_server(tcp_server_)
    , params(params_)
    , factory(factory_)
    , thread_pool(thread_pool_)
    , read_event(read_event_)
    , write_event(write_event_)
    , log(getLogger("HTTP2ServerConnection"))
    , peer_address(socket().peerAddress().toString())
    , socket_in(std::make_unique<ReadBufferFromPocoSocket>(socket(), read_event_))
    , socket_out(std::make_unique<WriteBufferFromPocoSocket>(socket(), write_event_))
    , stream_event_pipe(std::make_shared<Poco::Pipe>())
{
    poco_check_ptr(factory);
}

void HTTP2ServerConnection::run()
{
    try
    {
        doRun();
    }
    catch (const Poco::Exception & e)
    {
        LOG_ERROR(log, "Error in HTTP/2 connection with {}: {}", peer_address, e.displayText());
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(log, "Error in HTTP/2 connection with {}: {}", peer_address, e.what());
    }
    nghttp2_session_del(session);
    socket_out->finalize();
    LOG_INFO(log, "HTTP/2 connection with {} finished", peer_address);
}

void HTTP2ServerConnection::doRun()
{
    char client_preface[sizeof(HTTP2_CLIENT_PREFACE)];
    try
    {
        socket_in->readStrict(client_preface, sizeof(client_preface));
    }
    catch (const Poco::Exception &)
    {
        LOG_ERROR(log, "Could not read HTTP/2 client preface from {}", peer_address);
        return;
    }

    initSession();

    ssize_t preface_consumed = nghttp2_session_mem_recv(session, reinterpret_cast<const uint8_t *>(client_preface), sizeof(client_preface));
    if (preface_consumed < 0)
    {
        LOG_ERROR(log, "Error in HTTP/2 connection with {}: {}", peer_address, nghttp2_strerror(static_cast<int>(preface_consumed)));
        return;
    }
    LOG_INFO(log, "Established an HTTP/2 connection with {}. Start receiving frames", peer_address);

    sendPendingFrames();   /// Send initial SETTINGS frame

    bool choose_pipe = false;

    while (tcp_server.isOpen()
        && (nghttp2_session_want_read(session) || nghttp2_session_want_write(session)))
    {
        /*
        We wait for data either from socket or from stream event pipe
        If both are available for reading we choose one in a round-robin maner
        We must consider that data from socket might be buffered in the ReadBuffer
        */

        bool want_read_from_pipe = false;
        bool want_read_from_socket = false;

        {
            struct pollfd pfds[1];
            pfds[0].fd = stream_event_pipe->readHandle();
            pfds[0].events = POLLIN;
            int ret = poll(pfds, 1, 0);
            if (ret < 0)
            {
                LOG_ERROR(log, "HTTP/2 connection with {}: poll() returned {}", peer_address, ret);
                break;
            }
            want_read_from_pipe = (pfds[0].revents & POLLIN);
        }

        want_read_from_socket = socket_in->hasPendingData();
        if (!want_read_from_socket)
        {
            struct pollfd pfds[2];
            pfds[0].fd = socket().sockfd();
            pfds[0].events = POLLIN;
            pfds[1].fd = stream_event_pipe->readHandle();
            pfds[1].events = POLLIN;
            int ret = poll(pfds, 2, -1);
            if (ret < 0)
            {
                LOG_ERROR(log, "HTTP/2 connection with {}: poll() returned {}", peer_address, ret);
                break;
            }
            want_read_from_socket = (pfds[0].revents & POLLIN);
            want_read_from_pipe = (pfds[1].revents & POLLIN);
        }

        bool decided_to_read_from_pipe = false;
        chassert(want_read_from_socket || want_read_from_pipe);
        if (want_read_from_socket && want_read_from_pipe)
        {
            decided_to_read_from_pipe = choose_pipe;
            choose_pipe = !choose_pipe;
        }
        else if (want_read_from_socket)
            decided_to_read_from_pipe = false;
        else if (want_read_from_pipe)
            decided_to_read_from_pipe = true;

        if (decided_to_read_from_pipe)
        {
            if (!processNextStreamEvent())
                break;
        }
        else
        {
            if (!processNextFrame())
                break;
        }

        sendPendingFrames();
    }
}

void HTTP2ServerConnection::sendPendingFrames()
{
    while (nghttp2_session_want_write(session))
    {
        const uint8_t * data;
        ssize_t data_len = nghttp2_session_mem_send(session, &data);
        if (data_len < 0)
        {
            LOG_ERROR(log, "nghttp2_session_mem_send error: {}", nghttp2_strerror(data_len));
            break;
        }
        if (data_len == 0)
            break;
        socket_out->socketSendBytes(reinterpret_cast<const char *>(data), data_len);
    }
}

bool HTTP2ServerConnection::processNextFrame()
{
    buf = Memory(FRAME_HEADER_SIZE);
    try
    {
        socket_in->readStrict(buf.data(), FRAME_HEADER_SIZE);
    }
    catch (const Poco::Exception &)
    {
        LOG_ERROR(log, "Could not read HTTP/2 frame header from {}", peer_address);
        return false;
    }
    /// Frame format is defined in RFC 9113 section 4.1.
    uint32_t frame_size = (static_cast<uint32_t>(buf[0]) << 16) + (static_cast<uint32_t>(buf[1]) << 8) + static_cast<uint32_t>(buf[2]);
    /// If we get a larger frame size then the call to nghttp2_session_mem_recv should fail
    frame_size = std::min(frame_size, params->getMaxFrameSize());
    buf.resize(buf.size() + frame_size);
    try
    {
        socket_in->readStrict(buf.data() + FRAME_HEADER_SIZE, frame_size);
    }
    catch (const Poco::Exception &)
    {
        LOG_ERROR(log, "Could not read HTTP/2 frame payload from {}", peer_address);
        return false;
    }
    ssize_t consumed = nghttp2_session_mem_recv(session, reinterpret_cast<const uint8_t *>(buf.data()), FRAME_HEADER_SIZE + frame_size);
    if (consumed < 0)
    {
        LOG_ERROR(log, "Error in HTTP/2 connection with {}: {}", peer_address, nghttp2_strerror(static_cast<int>(consumed)));
        return false;
    }
    return true;
}

bool HTTP2ServerConnection::processNextStreamEvent()
{
    HTTP2StreamEvent event;
    stream_event_pipe->readBytes(&event, sizeof(event));
    if (event.type == HTTP2StreamEventType::SEND_100_CONTINUE)
        submit100Continue(event.stream_id);
    else if (event.type == HTTP2StreamEventType::OUTPUT_READY)
        onOutputReady(event.stream_id);
    else
    {
        LOG_ERROR(log, "Unknown HTTP/2 stream event type ({}) in connection with {}", event.type, peer_address);
        return false;
    }
    return true;
}

void HTTP2ServerConnection::submit100Continue(uint32_t stream_id)
{
    nghttp2_nv nv = {
        STATUS_PSEUDOHEADER,
        const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>("100")),
        sizeof(STATUS_PSEUDOHEADER), 3,
        NGHTTP2_NV_FLAG_NONE
    };

    /// FIXME: error handling
    nghttp2_submit_headers(session, NGHTTP2_FLAG_NONE, stream_id, nullptr, &nv, 1, nullptr);
}

void HTTP2ServerConnection::onOutputReady(uint32_t stream_id)
{
    auto it = streams.find(stream_id);
    chassert(it != streams.end() && it->second);
    HTTP2Stream & stream = *it->second;

    std::lock_guard lock(stream.output_mutex);
    chassert(stream.output != nullptr);

    if (!stream.response_submitted)
    {
        std::vector<nghttp2_nv> nva;
        std::string status = std::to_string(stream.response.getStatus());
        nva.push_back({
            STATUS_PSEUDOHEADER,
            reinterpret_cast<uint8_t *>(status.data()),
            sizeof(STATUS_PSEUDOHEADER), status.size(),
            NGHTTP2_NV_FLAG_NONE
        });
        prepareHeaders(stream, nva);
        const nghttp2_data_source source{.ptr=&stream};
        const nghttp2_data_provider2 data_prd{.source=source, .read_callback=dataSourceReadCallback};
        const nghttp2_data_provider2 * data_prd_ptr = (stream.end_stream && stream.output_len == 0) ? nullptr : &data_prd;
        nghttp2_submit_response2(session, stream.id, nva.data(), nva.size(), data_prd_ptr);
        LOG_INFO(log, "Submitting response for stream {} in connection with {}", stream_id, peer_address);
        stream.response_submitted = true;
        stream.response.markSendStarted();
        return;
    }
    if (stream.output_deferred)
    {
        stream.output_deferred = false;
        nghttp2_session_resume_data(session, stream.id);
    }
}

void HTTP2ServerConnection::prepareHeaders(HTTP2Stream & stream, std::vector<nghttp2_nv> & nva)
{
    nva.reserve(nva.size() + stream.response.size());
    for (const auto & header : stream.response)
    {
        if (strcasecmp(header.first.c_str(), "Connection") == 0 ||
            strcasecmp(header.first.c_str(), "Keep-Alive") == 0 ||
            strcasecmp(header.first.c_str(), "Transfer-Encoding") == 0)
            continue;

        nva.push_back({
            const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(header.first.data())),
            const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(header.second.data())),
            header.first.size(),
            header.second.size(),
            NGHTTP2_NV_FLAG_NONE
        });
    }
}

void HTTP2ServerConnection::initSession()
{
    nghttp2_session_callbacks * callbacks;
    nghttp2_session_callbacks_new(&callbacks);

    nghttp2_session_callbacks_set_on_begin_headers_callback(callbacks, onBeginHeadersCallback);
    nghttp2_session_callbacks_set_on_header_callback(callbacks, onHeaderCallback);
    nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks, onFrameRecvCallback);
    nghttp2_session_callbacks_set_on_stream_close_callback(callbacks, onStreamCloseCallback);
    nghttp2_session_callbacks_set_on_data_chunk_recv_callback(callbacks, onDataChunkRecvCallback);

    nghttp2_session_server_new(&session, callbacks, this);

    nghttp2_settings_entry settings[] = {
        {NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, params->getMaxConcurrentStreams()},
        {NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE, params->getInitialWindowSize()},
        {NGHTTP2_SETTINGS_MAX_HEADER_LIST_SIZE, static_cast<uint32_t>(context->getMaxFields())},
        {NGHTTP2_SETTINGS_MAX_FRAME_SIZE, params->getMaxFrameSize()},
    };

    nghttp2_submit_settings(session, NGHTTP2_FLAG_NONE, settings, sizeof(settings) / sizeof(settings[0]));

    nghttp2_session_callbacks_del(callbacks);
}

ssize_t HTTP2ServerConnection::dataSourceReadCallback(nghttp2_session * /*session*/, int32_t /*stream_id*/,
    uint8_t * buf, size_t length, uint32_t * data_flags,
    nghttp2_data_source * source, void * /*user_data*/)
{
    HTTP2Stream & stream = *reinterpret_cast<HTTP2Stream *>(source->ptr);
    std::lock_guard lock(stream.output_mutex);
    if (stream.output == nullptr)
    {
        if (stream.end_stream)
        {
            *data_flags = NGHTTP2_FLAG_END_STREAM;
            return 0;
        }
        stream.output_deferred = true;
        return NGHTTP2_ERR_DEFERRED;
    }
    size_t to_copy = std::min(length, stream.output_len - stream.output_consumed);
    /// FIXME: avoid copying using NGHTTP2_DATA_FLAG_NO_COPY
    memcpy(buf, stream.output->data() + stream.output_consumed, to_copy);
    stream.output_consumed += to_copy;
    if (stream.output_consumed < stream.output_len)
        return to_copy;
    if (stream.end_stream)
        *data_flags = NGHTTP2_FLAG_END_STREAM;
    stream.output_cv.notify_one();
    return to_copy;
}

int HTTP2ServerConnection::onBeginHeadersCallback(nghttp2_session * /*session*/,
    const nghttp2_frame * frame, void * user_data)
{
    HTTP2ServerConnection * self = reinterpret_cast<HTTP2ServerConnection *>(user_data);

    chassert(frame->hd.type == NGHTTP2_HEADERS && frame->headers.cat == NGHTTP2_HCAT_REQUEST);

    std::unique_ptr<HTTP2Stream> stream = std::make_unique<HTTP2Stream>(
        frame->hd.stream_id, self->stream_event_pipe, self->context, self->factory, self->forwarded_for, self->socket().peerAddress(), self->socket().address(), self->socket().secure(), self->socket().impl());

    self->streams.emplace(frame->hd.stream_id, std::move(stream));
    return 0;
}

int HTTP2ServerConnection::onHeaderCallback(nghttp2_session * /*session*/, const nghttp2_frame * frame,
    const uint8_t * name, size_t namelen, const uint8_t * value,
    size_t valuelen, uint8_t /*flags*/, void * user_data)
{
    HTTP2ServerConnection * self = reinterpret_cast<HTTP2ServerConnection *>(user_data);

    auto it = self->streams.find(frame->hd.stream_id);
    chassert(it != streams.end() && it->second);
    HTTP2Stream & stream = *it->second;

    /// FIXME: Double copying here and in request.set()
    std::string header_name(reinterpret_cast<const char *>(name), namelen);
    std::string header_value(reinterpret_cast<const char *>(value), valuelen);

    if (header_name == ":method")
        stream.request.setMethod(header_value);
    else if (header_name == ":scheme")
        {}
    else if (header_name == ":path")
        stream.request.setURI(header_value);
    else if (header_name == ":authority")
        stream.request.set("Host", header_value);
    else
        stream.request.set(header_name, header_value);

    return 0;
}

int HTTP2ServerConnection::onFrameRecvCallback(nghttp2_session * session,
    const nghttp2_frame * frame, void * user_data)
{
    HTTP2ServerConnection * self = reinterpret_cast<HTTP2ServerConnection *>(user_data);

    if (frame->hd.type != NGHTTP2_HEADERS || frame->headers.cat != NGHTTP2_HCAT_REQUEST)
        return 0;

    auto it = self->streams.find(frame->hd.stream_id);
    chassert(it != streams.end() && it->second);
    HTTP2Stream & stream = *it->second;

    if (frame->hd.flags & NGHTTP2_FLAG_END_STREAM)
        stream.eof = true;

    try
    {
        self->thread_pool.start(stream, Poco::format("HTTP/2 stream {} from connection with {}", frame->hd.stream_id, self->peer_address));
    }
    catch (const Poco::NoThreadAvailableException &)
    {
        nghttp2_submit_rst_stream(session, NGHTTP2_FLAG_NONE, frame->hd.stream_id, NGHTTP2_REFUSED_STREAM);
    }
    LOG_INFO(self->log, "Started new stream {} in HTTP/2 connection with {}", frame->hd.stream_id, self->peer_address);

    return 0;
}

int HTTP2ServerConnection::onStreamCloseCallback(nghttp2_session * /*session*/, int32_t stream_id,
    uint32_t /*error_code*/, void * user_data)
{
    HTTP2ServerConnection * self = reinterpret_cast<HTTP2ServerConnection *>(user_data);

    auto it = self->streams.find(stream_id);
    chassert(it != streams.end() && it->second);
    it->second->closed = true;

    return 0;
}

int HTTP2ServerConnection::onDataChunkRecvCallback(nghttp2_session * /* session*/, uint8_t flags,
    int32_t stream_id, const uint8_t * data,
    size_t len, void * user_data)
{
    HTTP2ServerConnection * self = reinterpret_cast<HTTP2ServerConnection *>(user_data);

    /// We feed nghttp2 full frames and expect it to not copy data
    chassert(self->buf.data() <= reinterpret_cast<const char *>(data) && reinterpret_cast<const char *>(data) <= self->buf.data() + self->buf.size());

    auto it = self->streams.find(stream_id);
    chassert(it != streams.end() && it->second);
    HTTP2Stream & stream = *it->second;

    std::lock_guard lock(stream.input_mutex);
    if (flags & NGHTTP2_FLAG_END_STREAM)
        stream.eof = true;
    stream.input.emplace_back(std::move(self->buf), len);
    stream.input_cv.notify_one();

    return 0;
}

}
