#include <Poco/Net/TCPServerConnectionFactory.h>
#include <Server/TCPServer.h>

namespace DB
{

class TCPServerConnectionFactoryImpl : public Poco::Net::TCPServerConnectionFactory
{
public:
    TCPServerConnectionFactoryImpl(TCPServer & tcp_server_, DB::TCPServerConnectionFactory::Ptr factory_)
        : tcp_server(tcp_server_)
        , factory(factory_)
    {}

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket) override
    {
        return factory->createConnection(socket, tcp_server);
    }
private:
    TCPServer & tcp_server;
    DB::TCPServerConnectionFactory::Ptr factory;
};

TCPServer::TCPServer(
    TCPServerConnectionFactory::Ptr factory_,
    Poco::ThreadPool & thread_pool,
    Poco::Net::ServerSocket & socket_,
    Poco::Net::TCPServerParams::Ptr params)
    : Poco::Net::TCPServer(new TCPServerConnectionFactoryImpl(*this, factory_), thread_pool, socket_, params)
    , factory(factory_)
    , socket(socket_)
    , is_open(true)
    , port_number(socket.address().port())
{}

}
