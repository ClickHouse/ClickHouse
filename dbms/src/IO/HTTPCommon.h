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


/// Call this method if you are going to make HTTPS requests. It's safe to call it many time from different threads.
void initSSL();


/// Create session object to perform requests and set required parameters.
std::unique_ptr<Poco::Net::HTTPClientSession> makeHTTPSession(const Poco::URI & uri, const ConnectionTimeouts & timeouts);


/** Used to receive response (response headers and possibly body)
  *  after sending data (request headers and possibly body).
  * Throws exception in case of non HTTP_OK (200) response code.
  * Returned istream lives in 'session' object.
  */
std::istream * receiveResponse(Poco::Net::HTTPClientSession & session, const Poco::Net::HTTPRequest & request, Poco::Net::HTTPResponse & response);

}
