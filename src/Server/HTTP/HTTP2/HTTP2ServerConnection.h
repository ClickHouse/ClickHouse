#pragma once

#include <Common/ProfileEvents.h>

#include <Server/HTTP/HTTP2/HTTP2ServerParams.h>
#include <Server/HTTP/HTTPContext.h>
#include <Server/HTTP/HTTPRequestHandlerFactory.h>
#include <Server/TCPServer.h>

#include <Poco/Net/SecureServerSocket.h>
#include <Poco/Net/TCPServerConnection.h>

namespace DB
{

bool setHTTP2Alpn(const Poco::Net::SecureServerSocket & socket, HTTP2ServerParams::Ptr http2_params);

bool isHTTP2Connection(const Poco::Net::StreamSocket & socket, HTTP2ServerParams::Ptr http2_params);

class HTTP2ServerConnection : public Poco::Net::TCPServerConnection
{
public:
    HTTP2ServerConnection(
        HTTPContextPtr context,
        TCPServer & tcp_server,
        const Poco::Net::StreamSocket & socket,
        HTTP2ServerParams::Ptr params,
        HTTPRequestHandlerFactoryPtr factory,
        const ProfileEvents::Event & read_event_ = ProfileEvents::end(),
        const ProfileEvents::Event & write_event_ = ProfileEvents::end());

    HTTP2ServerConnection(
        HTTPContextPtr context_,
        TCPServer & tcp_server_,
        const Poco::Net::StreamSocket & socket_,
        HTTP2ServerParams::Ptr params_,
        HTTPRequestHandlerFactoryPtr factory_,
        const String & forwarded_for_,
        const ProfileEvents::Event & read_event_ = ProfileEvents::end(),
        const ProfileEvents::Event & write_event_ = ProfileEvents::end())
    : HTTP2ServerConnection(context_, tcp_server_, socket_, params_, factory_, read_event_, write_event_)
    {
        forwarded_for = forwarded_for_;
    }

    void run() override;

private:
    [[maybe_unused]] HTTPContextPtr context;
    [[maybe_unused]] TCPServer & tcp_server;
    [[maybe_unused]] HTTP2ServerParams::Ptr params;
    [[maybe_unused]] HTTPRequestHandlerFactoryPtr factory;
    [[maybe_unused]] String forwarded_for;
    [[maybe_unused]] ProfileEvents::Event read_event;
    [[maybe_unused]] ProfileEvents::Event write_event;
    [[maybe_unused]] bool stopped;
    [[maybe_unused]] std::mutex mutex;  // guards the |factory| with assumption that creating handlers is not thread-safe.
};

}
