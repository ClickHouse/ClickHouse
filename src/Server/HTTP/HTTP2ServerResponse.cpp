#include <Server/HTTP/HTTP2ServerResponse.h>
#include <Server/HTTP/HTTP2ServerConnection.h>
#include <Server/HTTP/HTTP2ServerStream.h>
#include <Server/HTTP/HTTP2ServerSession.h>

namespace DB {

    void HTTP2ServerResponse::send() {
        stream->session->sendResponse(stream);
    }

    bool HTTP2ServerResponse::hasHeader(const std::string &name) {
        for (const auto &header : headers) {
            if (header.first == name) {
                return true;
            }
        }
        return false;
    }

    std::string HTTP2ServerResponse::getHeader(const std::string &name) {
        for (const auto &header : headers) {
            if (header.first == name) {
                return header.second;
            }
        }
        return "";
    }

    void HTTP2ServerResponse::addHeader(const std::string& name, const std::string& value) {
        headers.push_back({name, value});
    }
    
} // DB
