#include <Server/HTTP/HTTP2ServerSession.h>
#include <Server/HTTP/HTTP2ServerConnection.h>
#include <Server/HTTP/HTTP2ServerStream.h>

#include <cstdint>
#include <stdexcept>
#include <string_view>
#include <sys/socket.h>
#include "base/defines.h"

namespace DB {

    static int on_begin_headers_callback(nghttp2_session *session, const nghttp2_frame *frame, void *user_data) {
        UNUSED(session);

        auto http2_session = static_cast<HTTP2ServerSession*>(user_data);

        if (frame->hd.type != NGHTTP2_HEADERS ||
            frame->headers.cat != NGHTTP2_HCAT_REQUEST) {
            return 0;
        }

        http2_session->createStream(frame->hd.stream_id);

        return 0;
    }

    static int on_header_callback(nghttp2_session *session,
                                  const nghttp2_frame *frame, const uint8_t *name,
                                  size_t namelen, const uint8_t *value,
                                  size_t valuelen, uint8_t flags,
                                  void *user_data) {
        UNUSED(session);
        UNUSED(flags);

        auto http2_session = static_cast<HTTP2ServerSession*>(user_data);
        auto stream_id = frame->hd.stream_id;

        if (frame->hd.type != NGHTTP2_HEADERS ||
            frame->headers.cat != NGHTTP2_HCAT_REQUEST) {
            return 0;
        }

        auto stream = http2_session->getStream(stream_id);
        if (!stream) {
            return 0;
        }

        stream->request.addHeader(name, namelen, value, valuelen);

        return 0;
    }

    int on_frame_recv_callback(nghttp2_session *session, const nghttp2_frame *frame,
                               void *user_data) {
        UNUSED(session);

        auto http2_session = static_cast<HTTP2ServerSession*>(user_data);
        auto stream = http2_session->getStream(frame->hd.stream_id);

        if (!stream) {
            return 0;
        }

        switch (frame->hd.type) {
            case NGHTTP2_DATA:
                break;
            case NGHTTP2_HEADERS: {
                if (frame->headers.cat != NGHTTP2_HCAT_REQUEST) {
                    break;
                }
                if (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) {
                    http2_session->connection->handleRequest(stream);
                }
                break;
            }
            default:
                break;
        }

        return 0;
    }

    int on_stream_close_callback(nghttp2_session *session, int32_t stream_id,
                                 uint32_t error_code, void *user_data) {
        UNUSED(session);
        UNUSED(error_code);

        auto http2_session = static_cast<HTTP2ServerSession*>(user_data);
        auto stream = http2_session->getStream(stream_id);

        if (!stream) {
            return 0;
        }

        http2_session->removeStream(stream_id);
        return 0;
    }

    static nghttp2_nv addNV(std::string& name, std::string& value) {
        return {reinterpret_cast<uint8_t*>(name.data()), reinterpret_cast<uint8_t*>(value.data()), name.size(), value.size(),
                NGHTTP2_NV_FLAG_NO_COPY_NAME};
    }

    static nghttp2_ssize copyResponseData(nghttp2_session *session, int32_t stream_id,
        uint8_t *buf, size_t length, uint32_t *data_flags, nghttp2_data_source *source, void *user_data) {
        UNUSED(session);
        UNUSED(stream_id);
        UNUSED(user_data);
        
        auto stream = static_cast<HTTP2ServerStream*>(source->ptr);

        size_t r = stream->response.out.read(buf, length);

        if (r == 0) {
            *data_flags |= NGHTTP2_DATA_FLAG_EOF;
        }
        return static_cast<nghttp2_ssize>(r);
    }

    bool HTTP2ServerSession::sessionEnd() {

        /// TODO: refactor

        int wr = nghttp2_session_want_read(session);
        int ww = nghttp2_session_want_write(session);

        if (!wr && !ww) {
            return true;
        }
        return false;
    }

    int HTTP2ServerSession::initHTTP2Session() {
        nghttp2_session_callbacks *callbacks;

        int rv = nghttp2_session_callbacks_new(&callbacks);
        if (rv != 0) {
            nghttp2_session_callbacks_del(callbacks);
            return -1;
        }

        nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks,
                                                             on_frame_recv_callback);
        nghttp2_session_callbacks_set_on_stream_close_callback(
                callbacks, on_stream_close_callback);
        nghttp2_session_callbacks_set_on_header_callback(callbacks,
                                                         on_header_callback);
        nghttp2_session_callbacks_set_on_begin_headers_callback(
                callbacks, on_begin_headers_callback);


        rv = nghttp2_session_server_new(&session, callbacks, this);
        if (rv != 0) {
            nghttp2_session_callbacks_del(callbacks);
            return -1;
        }

        if (sendServerConnectionHeader() != 0 ||
            sessionSend() != 0) {
            nghttp2_session_callbacks_del(callbacks);
            return -1;
        }

        nghttp2_session_callbacks_del(callbacks);
        return 0;
    }

    HTTP2ServerSession::HTTP2ServerSession(
            HTTP2ServerConnection* connection_,
            const Poco::Net::StreamSocket & socket_
            )
        : connection(connection_)
        , socket(socket_)
        , input_buffer(100000)
        {   
            int rv = initHTTP2Session();
            if (rv != 0) {
                throw std::runtime_error("Session init error");
            }
        }

    HTTP2ServerSession::~HTTP2ServerSession() {
        nghttp2_session_del(session);
    }


    int HTTP2ServerSession::sessionSend() {
        for(;;) {
            const uint8_t *data_ptr;
            nghttp2_ssize n = nghttp2_session_mem_send(session, &data_ptr);

            if (n < 0) {
                return -1;
            }
            else if (n == 0) {
                return 0;
            }

            socket.sendBytes(data_ptr, static_cast<int>(n));
        }
    }

    int HTTP2ServerSession::sendServerConnectionHeader() {
        nghttp2_settings_entry iv[1] = {
                {NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, 100}};

        int rv;
        rv = nghttp2_submit_settings(session, NGHTTP2_FLAG_NONE, iv, 1);
        return rv;
    }

    int HTTP2ServerSession::sessionReceive() {
        if (!input_buffer.isReadable()) {
            if (input_buffer.isWritable()) {
                std::array<char, buf_input_size> buf_data;
                int n = socket.receiveBytes(buf_data.data(), buf_input_size);
                input_buffer.write(buf_data.data(), n);
            } else {
                return -1;
            }
        }
        
        ssize_t readlen = nghttp2_session_mem_recv(session, reinterpret_cast<uint8_t*>(input_buffer.begin()), input_buffer.size());
        if (readlen < 0) {
            return -1;
        }

        if (readlen) {
            input_buffer.drain(readlen);
        }

        return sessionSend();
    }

    void HTTP2ServerSession::removeStream(int stream_id) {
        streams.erase(stream_id);
    }

    void HTTP2ServerSession::createStream(int stream_id) {
        streams.emplace(stream_id, std::make_unique<HTTP2ServerStream>(stream_id, this));
    }

    HTTP2ServerStream* HTTP2ServerSession::getStream(int stream_id) {
        if (auto it = streams.find(stream_id); it != streams.end()) {
            return it->second.get();
        }
        return nullptr;
    }

    void HTTP2ServerSession::sendResponse(HTTP2ServerStream *stream) {
        auto nva = std::vector<nghttp2_nv>();

        for (auto& hd : stream->response.headers) {
            nva.push_back(addNV(hd.first, hd.second));
        }

        nghttp2_data_provider *prd_ptr = nullptr, prd;

        if (stream->response.out.isReadable()) {
            prd.source.ptr = stream;
            prd.read_callback = copyResponseData;
            prd_ptr = &prd;
        }

        int rv = nghttp2_submit_response(session, stream->stream_id, nva.data(),
                                     nva.size(), prd_ptr);
        
        if (rv != 0) {
            // TODO: handle error
        }
        
        sessionSend();
    }
}
