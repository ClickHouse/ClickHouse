#pragma once

#include <Server/HTTP/HTTPServerRequest.h>
#include <Server/HTTP/HTTPServerResponseBase.h>

#include <boost/noncopyable.hpp>

namespace DB
{

class HTTPRequestHandler : private boost::noncopyable
{
public:
    virtual ~HTTPRequestHandler() = default;

    virtual void handleRequest(HTTPServerRequest & request, HTTPServerResponseBase & response) = 0;
};

}
