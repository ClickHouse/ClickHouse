#pragma once

#include <Core/Types.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <IO/WriteBufferFromHTTPServerResponse.h>

namespace DB
{

class HTTPExceptionHandler
{
public:
    static void handle(const std::string & message, int exception_code, Poco::Net::HTTPServerRequest & request,
        Poco::Net::HTTPServerResponse & response, std::shared_ptr<WriteBufferFromHTTPServerResponse> response_out, bool compression,
        Poco::Logger * log);
};

}
