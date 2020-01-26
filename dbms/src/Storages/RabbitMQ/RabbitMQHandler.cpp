#include <Poco/Net/StreamSocket.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>

namespace DB
{

/// to connect to rabbitmq server
RabbitMQHandler::RabbitMQHandler(const std::string& host, uint16_t port) :
        m_impl(new RabbitMQHandlerImpl)
{
    const Poco::Net::SocketAddress address(host, port);
    m_impl->socket.connect(address);
    m_impl->socket.setKeepAlive(true);
}


///     These functions are to be implemented

RabbitMQHandler::~RabbitMQHandler()
{
    close();
}

void RabbitMQHandler::loop()
{
    try
    {
        while (!m_impl->quit)
        {
            if (m_impl->socket.available() > 0)
            {
                /// recieve ans write data
            }
            if(m_impl->socket.available()<0)
            {
                /// throw socket error
            }
            /// parse and send data
        }

    } catch (const Poco::Exception& exc)
    {
        std::cerr<< "Poco exception " << exc.displayText();
    }
}

void RabbitMQHandler::quit()
{
    m_impl->quit = true;
}

void RabbitMQHandler::RabbitMQHandler::close()
{
    m_impl->socket.close();
}

void RabbitMQHandler::onData(AMQP::Connection *connection, const char *data, size_t size)
{
    m_impl->connection = connection;
    if (data && size)
    {
        /**
        const size_t writen = m_impl->outBuffer.write(data, size);
        if (writen != size)
        {
            sendDataFromBuffer();
            m_impl->outBuffer.write(data + writen, size - writen);
        } */
    }
}

void RabbitMQHandler::onConnected(AMQP::Connection */*connection*/)
{
    m_impl->connected = true;
}

void RabbitMQHandler::onError(AMQP::Connection * /*connection*/, const char */*message*/)
{
}

void RabbitMQHandler::onClosed(AMQP::Connection */*connection*/)
{
    m_impl->quit  = true;
}


bool RabbitMQHandler::connected() const
{
    return m_impl->connected;
}
}