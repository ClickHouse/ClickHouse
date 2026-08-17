#pragma once

#include <Server/HTTP/HTTPRequestHandlerFactory.h>
#include <Server/HTTP/HTTPContext.h>

#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/HTTPServerSession.h>
#include <Poco/Net/TCPServerConnection.h>

namespace DB
{
class TCPServer;

class HTTPServerConnection : public Poco::Net::TCPServerConnection
{
public:
    HTTPServerConnection(
        HTTPContextPtr context,
        TCPServer & tcp_server,
        const Poco::Net::StreamSocket & socket,
        Poco::Net::HTTPServerParams::Ptr params,
        HTTPRequestHandlerFactoryPtr factory,
        const ProfileEvents::Event & read_event_ = ProfileEvents::end(),
        const ProfileEvents::Event & write_event_ = ProfileEvents::end());

    HTTPServerConnection(
        HTTPContextPtr context_,
        TCPServer & tcp_server_,
        const Poco::Net::StreamSocket & socket_,
        Poco::Net::HTTPServerParams::Ptr params_,
        HTTPRequestHandlerFactoryPtr factory_,
        const String & forwarded_for_,
        const ProfileEvents::Event & read_event_ = ProfileEvents::end(),
        const ProfileEvents::Event & write_event_ = ProfileEvents::end())
    : HTTPServerConnection(context_, tcp_server_, socket_, params_, factory_, read_event_, write_event_)
    {
        forwarded_for = forwarded_for_;
    }

    void run() override;

protected:
    static void sendErrorResponse(Poco::Net::HTTPServerSession & session, Poco::Net::HTTPResponse::HTTPStatus status);

private:
    HTTPContextPtr context;
    TCPServer & tcp_server;
    Poco::Net::HTTPServerParams::Ptr params;
    HTTPRequestHandlerFactoryPtr factory;
    String forwarded_for;
    ProfileEvents::Event read_event;
    ProfileEvents::Event write_event;
    bool stopped;
    std::mutex mutex;  // guards the |factory| with assumption that creating handlers is not thread-safe.
};

}
