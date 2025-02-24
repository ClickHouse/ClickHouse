#pragma once

#include <Poco/FIFOBuffer.h>

#include <map>
#include <string>

namespace DB {

    class HTTP2ServerConnection;

    class HTTP2ServerStream;

    class HTTP2ServerResponse {
    public:
        explicit HTTP2ServerResponse(HTTP2ServerStream* stream_) : stream(stream_), out(2048) {}

        void send();

        bool hasHeader(const std::string& name);

        std::string getHeader(const std::string& name);

        void addHeader(const std::string& name, const std::string& value);

        HTTP2ServerStream* stream;
        std::vector<std::pair<std::string, std::string>> headers;
        Poco::BasicFIFOBuffer<uint8_t> out;
    };

} // DB
