#pragma once

#include <map>
#include <string>

namespace DB {

    class HTTP2ServerRequest {
    public:
        void addHeader(const u_int8_t* name, size_t namelen, const u_int8_t* value, size_t valuelen);

        std::string getHeader(const std::string& name);

    private:
        std::map<std::string, std::string> headers;
    };

} // DB
