#include <Server/TCPServer.h>

#include <Poco/Net/TCPServerConnectionFactory.h>

namespace DB
{

class TCPServerConnectionFactoryImpl : public Poco::Net::TCPServerConnectionFactory
{
public:
    TCPServerConnectionFactoryImpl(TCPServer & tcp_server_, DB::TCPServerConnectionFactory::Ptr factory_)
        : tcp_server(tcp_server_), factory(factory_) {}

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket) override
    {
        return factory->createConnection(socket, tcp_server);
    }

private:
    TCPServer & tcp_server;
    DB::TCPServerConnectionFactory::Ptr factory;
};

TCPServer::TCPServer(
    const std::string & listen_host_,
    const std::string & port_name_,
    const std::string & description_,
    TCPServerConnectionFactory::Ptr factory_,
    Poco::ThreadPool & thread_pool,
    Poco::Net::ServerSocket & socket_,
    Poco::Net::TCPServerParams::Ptr params)
    : IProtocolServer(socket_.address().port(), port_name_, listen_host_, description_)
    , Poco::Net::TCPServer(new TCPServerConnectionFactoryImpl(*this, factory_), thread_pool, socket_, params)
    , factory(factory_)
    , socket(socket_)
{}

void TCPServer::stop()
{
    Poco::Net::TCPServer::stop();

    // This notifies already established connections that they should stop serving
    // queries and close their socket as soon as they can.
    IProtocolServer::stop();

    // Poco's stop() stops listening on the socket but leaves it open.
    // To be able to hand over control of the listening port to a new server, and
    // to get fast connection refusal instead of timeouts, we also need to close
    // the listening socket.
    socket.close();
}

}
