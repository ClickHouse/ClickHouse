#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>

#include <boost/noncopyable.hpp>

namespace DB
{

class HTTPRequestHandlerFactory : private boost::noncopyable
{
public:
    virtual ~HTTPRequestHandlerFactory() = default;

    virtual std::unique_ptr<HTTPRequestHandler> createRequestHandler(const HTTPServerRequest & request) = 0;
};

using HTTPRequestHandlerFactoryPtr = std::shared_ptr<HTTPRequestHandlerFactory>;

}
