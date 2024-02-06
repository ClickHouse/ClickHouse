#pragma once

#include <Poco/Net/NetException.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Common/logger_useful.h>
#include "Server/TCPProtocolStackData.h"
#include <Server/IServer.h>
#include <Server/TCPHandler.h>
#include <Server/TCPServerConnectionFactory.h>

namespace Poco { class Logger; }

namespace DB
{

class TCPHandlerFactory : public TCPServerConnectionFactory
{
private:
    IServer & server;
    bool parse_proxy_protocol = false;
    LoggerPtr log;
    std::string host_name;
    std::string server_display_name;

    ProfileEvents::Event read_event;
    ProfileEvents::Event write_event;

    class DummyTCPHandler : public Poco::Net::TCPServerConnection
    {
    public:
        using Poco::Net::TCPServerConnection::TCPServerConnection;
        void run() override {}
    };

public:
    /** parse_proxy_protocol_ - if true, expect and parse the header of PROXY protocol in every connection
      * and set the information about forwarded address accordingly.
      * See https://github.com/wolfeidau/proxyv2/blob/master/docs/proxy-protocol.txt
      */
    TCPHandlerFactory(IServer & server_, bool secure_, bool parse_proxy_protocol_, const ProfileEvents::Event & read_event_ = ProfileEvents::end(), const ProfileEvents::Event & write_event_ = ProfileEvents::end())
        : server(server_), parse_proxy_protocol(parse_proxy_protocol_)
        , log(getLogger(std::string("TCP") + (secure_ ? "S" : "") + "HandlerFactory"))
        , read_event(read_event_)
        , write_event(write_event_)
    {
        host_name = getFQDNOrHostName();
        server_display_name = server.config().getString("display_name", host_name);
    }

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server) override
    {
        try
        {
            LOG_TRACE(log, "TCP Request. Address: {}", socket.peerAddress().toString());
            return new TCPHandler(server, tcp_server, socket, parse_proxy_protocol, server_display_name, host_name, read_event, write_event);
        }
        catch (const Poco::Net::NetException &)
        {
            LOG_TRACE(log, "TCP Request. Client is not connected (most likely RST packet was sent).");
            return new DummyTCPHandler(socket);
        }
    }

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server, TCPProtocolStackData & stack_data) override
    {
        try
        {
            LOG_TRACE(log, "TCP Request. Address: {}", socket.peerAddress().toString());
            return new TCPHandler(server, tcp_server, socket, stack_data, server_display_name, host_name, read_event, write_event);
        }
        catch (const Poco::Net::NetException &)
        {
            LOG_TRACE(log, "TCP Request. Client is not connected (most likely RST packet was sent).");
            return new DummyTCPHandler(socket);
        }
    }
};

}
