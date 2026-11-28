
#include "HTTP2ServerRequest.h"

namespace DB {
    void HTTP2ServerRequest::addHeader(const u_int8_t* name, size_t namelen, const u_int8_t* value, size_t valuelen) {
        std::string name_str(reinterpret_cast<const char*>(name), namelen);
        std::string value_str(reinterpret_cast<const char*>(value), valuelen);
        headers.insert({name_str, value_str});
    }

    std::string HTTP2ServerRequest::getHeader(const std::string &name) {
        if (auto it = headers.find(name); it != headers.end()) {
            return it->second;
        }
        return "";
    }
} // DB
