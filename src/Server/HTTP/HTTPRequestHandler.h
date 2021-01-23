#pragma once

#include <Server/HTTP/HTTPServerRequest.h>
#include <Server/HTTP/HTTPServerResponse.h>

#include <boost/noncopyable.hpp>

namespace DB
{

class HTTPRequestHandler : private boost::noncopyable
{
public:
    virtual ~HTTPRequestHandler() = default;

    virtual void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) = 0;
};

}
