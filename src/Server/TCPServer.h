#pragma once

#include <string>
#include <Poco/Net/TCPServer.h>

#include <Server/IProtocolServer.h>
#include <Server/TCPServerConnectionFactory.h>
#include <base/types.h>


namespace DB
{
class Context;

class TCPServer : public Poco::Net::TCPServer, public IProtocolServer
{
public:
    explicit TCPServer(
        TCPServerConnectionFactory::Ptr factory,
        Poco::ThreadPool & thread_pool,
        Poco::Net::ServerSocket & socket,
        const std::string & listen_host_,
        const char * port_name_,
        const std::string & description_,
        Poco::Net::TCPServerParams::Ptr params = new Poco::Net::TCPServerParams );
    
    explicit TCPServer(
        TCPServerConnectionFactory::Ptr factory,
        Poco::ThreadPool & thread_pool,
        Poco::Net::ServerSocket & socket,
        Poco::Net::TCPServerParams::Ptr params = new Poco::Net::TCPServerParams );
    /// Close the socket and ask existing connections to stop serving queries
    void stop() override
    {
        Poco::Net::TCPServer::stop();
        // This notifies already established connections that they should stop serving
        // queries and close their socket as soon as they can.
        is_open = false;
        // Poco's stop() stops listening on the socket but leaves it open.
        // To be able to hand over control of the listening port to a new server, and
        // to get fast connection refusal instead of timeouts, we also need to close
        // the listening socket.
        socket.close();
    }

    void start() override { Poco::Net::TCPServer::start(); }
    
    bool isStopping() const override { return !is_open; }

    size_t currentConnections() const override { return Poco::Net::TCPServer::currentConnections(); }
    
    size_t currentThreads() const override { return Poco::Net::TCPServer::currentThreads(); }

    UInt16 portNumber() const override { return port_number; }

private:
    TCPServerConnectionFactory::Ptr factory;
    Poco::Net::ServerSocket socket;
    std::atomic<bool> is_open;
    UInt16 port_number;
    std::string listen_host;
    std::string port_name;
    std::string description;
};

}
