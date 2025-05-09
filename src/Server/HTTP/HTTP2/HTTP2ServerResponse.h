#pragma once

#include <Server/HTTP/HTTPServerResponse.h>

namespace DB
{

class HTTP2Stream;

class HTTP2ServerResponse : public HTTPServerResponseBase
{
public:
    explicit HTTP2ServerResponse(HTTP2Stream & stream_) : stream(stream_) {}

    void send100Continue();

private:
    WriteBufferFromHTTPServerResponseBase * makeNewStream() noexcept override;

    HTTP2Stream & stream;
};

}
