#include <Server/HTTP/deferHTTP100Continue.h>
#include <Server/HTTP/HTTPServerConnection.h>
#include <Server/TCPServer.h>

#include <Poco/Net/NetException.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>


namespace ProfileEvents
{
    extern const Event HTTPServerConnectionsCreated;
    extern const Event HTTPServerConnectionsReused;
    extern const Event HTTPServerConnectionsPreserved;
    extern const Event HTTPServerConnectionsExpired;
    extern const Event HTTPServerConnectionsClosed;
    extern const Event HTTPServerConnectionsReset;
}


namespace DB
{

HTTPServerConnection::HTTPServerConnection(
    HTTPContextPtr context_,
    TCPServer & tcp_server_,
    const Poco::Net::StreamSocket & socket,
    Poco::Net::HTTPServerParams::Ptr params_,
    HTTPRequestHandlerFactoryPtr factory_,
    const ProfileEvents::Event & read_event_,
    const ProfileEvents::Event & write_event_)
    : TCPServerConnection(socket), context(std::move(context_)), tcp_server(tcp_server_), params(params_), factory(factory_), read_event(read_event_), write_event(write_event_), stopped(false)
{
    poco_check_ptr(factory);
}

void HTTPServerConnection::run()
{
    std::string server = params->getSoftwareVersion();
    Poco::Net::HTTPServerSession session(socket(), params);

    ProfileEvents::increment(ProfileEvents::HTTPServerConnectionsCreated);

    while (!stopped && tcp_server.isOpen() && session.connected())
    {
        const bool is_first_request = params->getMaxKeepAliveRequests() == session.getMaxKeepAliveRequests();

        if (!session.hasMoreRequests())
        {
            if (is_first_request)
                // it is strange to have a connection being opened but no request has been sent, account it as an error case
                ProfileEvents::increment(ProfileEvents::HTTPServerConnectionsReset);
            else
                ProfileEvents::increment(ProfileEvents::HTTPServerConnectionsExpired);

            return;
        }
        else
        {
            if (!is_first_request)
                ProfileEvents::increment(ProfileEvents::HTTPServerConnectionsReused);
        }

        try
        {
            std::lock_guard lock(mutex);
            if (!stopped && tcp_server.isOpen() && session.connected())
            {
                HTTPServerResponse response(session);
                HTTPServerRequest request(context, response, session, read_event);

                Poco::Timestamp now;

                if (!forwarded_for.empty())
                    request.set("X-Forwarded-For", forwarded_for);

                if (request.isSecure())
                {
                    size_t hsts_max_age = context->getMaxHstsAge();

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
                    if (!tcp_server.isOpen())
                    {
                        sendErrorResponse(session, Poco::Net::HTTPResponse::HTTP_SERVICE_UNAVAILABLE);
                        break;
                    }
                    std::unique_ptr<HTTPRequestHandler> handler(factory->createRequestHandler(request));

                    if (handler)
                    {
                        if (!shouldDeferHTTP100Continue(request) && request.getExpectContinue() && response.getStatus() == Poco::Net::HTTPResponse::HTTP_OK)
                            response.sendContinue();

                        handler->handleRequest(request, response, write_event);

                        bool keep_alive = false;
                        if (!params->getKeepAlive() || !request.canKeepAlive())
                        {
                            /// Either server is not configured to keep connections alive or client did not ask it
                            ProfileEvents::increment(ProfileEvents::HTTPServerConnectionsClosed);
                        }
                        else if (session.getMaxKeepAliveRequests() == 0 || !session.canKeepAlive())
                        {
                            /// connection is expired by max request count
                            ProfileEvents::increment(ProfileEvents::HTTPServerConnectionsExpired);
                        }
                        else if (!response.getKeepAlive())
                        {
                            /// server decided to close connection
                            /// usually it is related to the cases:
                            /// - the request or response stream is not bounded or
                            /// - not all data is read from them
                            ProfileEvents::increment(ProfileEvents::HTTPServerConnectionsReset);
                        }
                        else
                        {
                            ProfileEvents::increment(ProfileEvents::HTTPServerConnectionsPreserved);
                            keep_alive = true;
                        }

                        session.setKeepAlive(keep_alive);
                    }
                    else
                    {
                        sendErrorResponse(session, Poco::Net::HTTPResponse::HTTP_NOT_IMPLEMENTED);
                    }
                }
                catch (Poco::Exception &)
                {
                    if (!response.sent())
                    {
                        try
                        {
                            sendErrorResponse(session, Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
                        }
                        catch (...) // NOLINT(bugprone-empty-catch)
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
        catch (const Poco::Net::MessageException & e)
        {
            LOG_DEBUG(LogFrequencyLimiter(getLogger("HTTPServerConnection"), 10), "HTTP request failed: {}: {}", HTTPResponse::HTTP_REASON_BAD_REQUEST, e.displayText());
            sendErrorResponse(session, Poco::Net::HTTPResponse::HTTP_BAD_REQUEST, e.message());
        }
        catch (const Poco::Net::NetException & e)
        {
            /// Do not spam logs with messages related to connection reset by peer.
            if (e.code() == POCO_ENOTCONN)
            {
                LOG_DEBUG(LogFrequencyLimiter(getLogger("HTTPServerConnection"), 10), "Connection reset by peer while processing HTTP request: {}", e.message());
                break;
            }

            if (session.networkException())
                session.networkException()->rethrow();
            else
                throw;
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
void HTTPServerConnection::sendErrorResponse(Poco::Net::HTTPServerSession & session, Poco::Net::HTTPResponse::HTTPStatus status, const std::string & message)
{
    HTTPServerResponse response(session);
    response.setVersion(Poco::Net::HTTPMessage::HTTP_1_1);
    response.setStatusAndReason(status);
    response.setKeepAlive(false);

    if (!message.empty())
    {
        response.setContentLength(message.size());
        response.setContentType("text/plain");
    }

    auto out = response.send();

    if (!message.empty())
        out->write(message.data(), message.size());

    session.setKeepAlive(false);
    ProfileEvents::increment(ProfileEvents::HTTPServerConnectionsReset);
}

}
