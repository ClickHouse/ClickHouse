#pragma once

#include <mutex>

namespace Poco
{
    namespace Net
    {
        class HTTPServerResponse;
    }
}


namespace DB
{

void setResponseDefaultHeaders(Poco::Net::HTTPServerResponse & response, int keep_alive_timeout);

extern std::once_flag ssl_init_once;
void SSLInit();

}
