#pragma once

#include <memory>
#include <Server/HTTP/HTTP2RequestHandler.h>

#include <boost/noncopyable.hpp>
#include <Server/IServer.h>
#include <base/defines.h>

namespace DB
{

class HTTP2RequestHandlerFactory : private boost::noncopyable
{
public:
    virtual ~HTTP2RequestHandlerFactory() = default;

    virtual std::unique_ptr<HTTP2RequestHandler> createRequestHandler(const HTTP2ServerRequest & request) = 0;
};

using HTTP2RequestHandlerFactoryPtr = std::shared_ptr<HTTP2RequestHandlerFactory>;

template <class Handler>
class SimpleHTTP2HandlerFactory : public HTTP2RequestHandlerFactory
{
public:
    explicit SimpleHTTP2HandlerFactory(IServer& server_) : server(server_) {}

    std::unique_ptr<HTTP2RequestHandler> createRequestHandler(const HTTP2ServerRequest & request) override {
        UNUSED(request);
        return std::make_unique<Handler>(server, 200);
    }

private:
    IServer& server;
};

}
