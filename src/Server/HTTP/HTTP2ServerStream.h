#pragma once

#include <Server/HTTP/HTTP2ServerRequest.h>
#include <Server/HTTP/HTTP2ServerResponse.h>

#include <memory>

namespace DB {

    class HTTP2ServerSession;

    class HTTP2ServerStream {
    public:
        HTTP2ServerStream(int stream_id_, HTTP2ServerSession* session_)
        : stream_id(stream_id_)
        , session(session_)
        , response(this)
        {}

        int stream_id;
        HTTP2ServerSession* session;
        HTTP2ServerRequest request;
        HTTP2ServerResponse response;
    };

} // DB

