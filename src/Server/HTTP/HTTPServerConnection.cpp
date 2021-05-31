#include <Server/HTTP/HTTPServerConnection.h>

#include <Poco/Net/NetException.h>

namespace DB
{

HTTPServerConnection::HTTPServerConnection(
    const Context & context_,
    const Poco::Net::StreamSocket & socket,
    Poco::Net::HTTPServerParams::Ptr params_,
    HTTPRequestHandlerFactoryPtr factory_)
    : TCPServerConnection(socket), context(context_), params(params_), factory(factory_), stopped(false)
{
    poco_check_ptr(factory);
}

void HTTPServerConnection::run()
{
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
                HTTPServerRequest request(context, response, session);

                Poco::Timestamp now;
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
        catch (Poco::Net::NoMessageException &)
        {
            break;
        }
        catch (Poco::Net::MessageException &)
        {
            sendErrorResponse(session, Poco::Net::HTTPResponse::HTTP_BAD_REQUEST);
        }
        catch (Poco::Exception &)
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

void HTTPServerConnection::onServerStopped(const bool & abortCurrent)
{
    stopped = true;
    if (abortCurrent)
    {
        try
        {
            socket().shutdown();
        }
        catch (...)
        {
        }
    }
    else
    {
        std::unique_lock<std::mutex> lock(mutex);

        try
        {
            socket().shutdown();
        }
        catch (...)
        {
        }
    }
}

}
