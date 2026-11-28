#include "StaticRequestHandler2.h"

#include <string>
#include "base/defines.h"

namespace DB
{
    void StaticRequestHandler2::handleRequest(HTTP2ServerRequest & request, HTTP2ServerResponse & response) {
        UNUSED(request);
        response.addHeader(":status", std::to_string(status));
        uint8_t data[11] = "HTTP/2 OK\n";
        response.out.write(data, 10);
        response.send();
    }
}
