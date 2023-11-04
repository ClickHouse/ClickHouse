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

    virtual void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const CurrentMetrics::Metric & write_metric) = 0;
    virtual void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) { handleRequest(request, response, CurrentMetrics::end()); }
};

}
