#pragma once

#include <mutex>
#include <memory>
#include <iostream>

#include <Poco/Net/HTTPClientSession.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/URI.h>


#include <IO/ConnectionTimeouts.h>

namespace Poco
{
    namespace Net
    {
        class HTTPServerResponse;
    }
}


namespace DB
{


const int HTTP_TOO_MANY_REQUESTS = 429;

void setResponseDefaultHeaders(Poco::Net::HTTPServerResponse & response, unsigned keep_alive_timeout);

extern std::once_flag ssl_init_once;
void SSLInit();

std::unique_ptr<Poco::Net::HTTPClientSession> getPreparedSession(const Poco::URI & uri, const ConnectionTimeouts & timeouts);

/* Function makes HTTP-request from prepared structures and returns response istream
 * in case of HTTP_OK and throws exception with details in case of not HTTP_OK
 */
std::istream* makeRequest(Poco::Net::HTTPClientSession & session, const Poco::Net::HTTPRequest & request, Poco::Net::HTTPResponse & response);
}
