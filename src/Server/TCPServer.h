#pragma once

#include <Poco/Net/TCPServer.h>

#include <base/types.h>
#include <Server/IProtocolServer.h>
#include <Server/TCPServerConnectionFactory.h>

namespace DB
{
class Context;

class TCPServer : public IProtocolServer, public Poco::Net::TCPServer
{
public:
    explicit TCPServer(
        const std::string & listen_host_,
        const std::string & port_name_,
        const std::string & description_,
        TCPServerConnectionFactory::Ptr factory,
        Poco::ThreadPool & thread_pool,
        Poco::Net::ServerSocket & socket,
        Poco::Net::TCPServerParams::Ptr params = new Poco::Net::TCPServerParams);

    void start() override { Poco::Net::TCPServer::start(); }

    void stop() override;

    size_t currentThreads() const override { return Poco::Net::TCPServer::currentThreads(); }

    size_t currentConnections() const override { return Poco::Net::TCPServer::currentConnections(); }

private:
    TCPServerConnectionFactory::Ptr factory;
    Poco::Net::ServerSocket socket;
};

}
