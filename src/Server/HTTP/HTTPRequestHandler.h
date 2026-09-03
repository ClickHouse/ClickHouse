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

    virtual void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) = 0;
    virtual void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) { handleRequest(request, response, ProfileEvents::end()); }
};

}
