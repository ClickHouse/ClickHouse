#include <Server/HTTP/HTTPServerConnection.h>
#include <Server/ProxyProtocolHandler.h>

#include <Poco/Net/NetException.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int IP_ADDRESS_NOT_ALLOWED;
}

HTTPServerConnection::HTTPServerConnection(
    ContextPtr context_,
    const Poco::Net::StreamSocket & socket,
    Poco::Net::HTTPServerParams::Ptr params_,
    HTTPRequestHandlerFactoryPtr factory_,
    const HTTPInterfaceConfigBase & config_
)
    : IndirectHTTPServerConnection(config_.name, socket, config_.proxies)
    , context(Context::createCopy(context_))
    , params(params_)
    , factory(factory_)
    , stopped(false)
    , config(config_)
{
    poco_check_ptr(factory);
}

void HTTPServerConnection::run()
{
    // Try to read the forwarded address from the raw stream, but check the result against allow_direct
    // only after the possible alternative info is extracted at each HTTP request later.
    handleProxyProtocol(socket());

    std::string server = params->getSoftwareVersion();
    Poco::Net::HTTPServerSession session(socket(), params);

    while (!stopped && session.hasMoreRequests())
    {
        try
        {
            std::unique_lock<std::mutex> lock(mutex);
            if (!stopped)
            {
                HTTPServerResponse response(session);
                HTTPServerRequest request(context, response, session, *this);

                handleProxyProtocol(request);
                if (!config.allow_direct && !isIndirect())
                {
                    try
                    {
                        sendErrorResponse(session, Poco::Net::HTTPResponse::HTTP_UNAUTHORIZED);
                    }
                    catch (...)
                    {
                    }
                    throw Exception("Direct connections are not allowed on the interface", ErrorCodes::IP_ADDRESS_NOT_ALLOWED);
                }

                Poco::Timestamp now;

                if (request.isSecure())
                {
                    size_t hsts_max_age = context->getSettings().hsts_max_age.value;

                    if (hsts_max_age > 0)
                        response.add("Strict-Transport-Security", "max-age=" + std::to_string(hsts_max_age));

                }

                response.setDate(now);
                response.setVersion(request.getVersion());
                response.setKeepAlive(params->getKeepAlive() && request.getKeepAlive() && session.canKeepAlive());
                if (!server.empty())
                    response.set("Server", server);
                try
                {
                    std::unique_ptr<HTTPRequestHandler> handler(factory->createRequestHandler(request));

                    if (handler)
                    {
                        if (request.getExpectContinue() && response.getStatus() == Poco::Net::HTTPResponse::HTTP_OK)
                            response.sendContinue();

                        handler->handleRequest(request, response);
                        session.setKeepAlive(params->getKeepAlive() && response.getKeepAlive() && session.canKeepAlive());
                    }
                    else
                        sendErrorResponse(session, Poco::Net::HTTPResponse::HTTP_NOT_IMPLEMENTED);
                }
                catch (Poco::Exception &)
                {
                    if (!response.sent())
                    {
                        try
                        {
                            sendErrorResponse(session, Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
                        }
                        catch (...)
                        {
                        }
                    }
                    throw;
                }
            }
        }
        catch (const Poco::Net::NoMessageException &)
        {
            break;
        }
        catch (const Poco::Net::MessageException &)
        {
            sendErrorResponse(session, Poco::Net::HTTPResponse::HTTP_BAD_REQUEST);
        }
        catch (const Poco::Exception &)
        {
            if (session.networkException())
            {
                session.networkException()->rethrow();
            }
            else
                throw;
        }
    }
}

// static
void HTTPServerConnection::sendErrorResponse(Poco::Net::HTTPServerSession & session, Poco::Net::HTTPResponse::HTTPStatus status)
{
    HTTPServerResponse response(session);
    response.setVersion(Poco::Net::HTTPMessage::HTTP_1_1);
    response.setStatusAndReason(status);
    response.setKeepAlive(false);
    response.send();
    session.setKeepAlive(false);
}

}
