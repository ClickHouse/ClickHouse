#include <Poco/Net/StreamSocket.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>
#include <common/logger_useful.h>

#include <stdio.h>
namespace DB
{

/// to connect to rabbitmq server
RabbitMQHandler::RabbitMQHandler(const std::pair<std::string, UInt16> & parsed_host_port, Poco::Logger * log_) :
        log(log_),
        handler_impl(new ConnectionImpl)
{
    const Poco::Net::SocketAddress address(parsed_host_port.first, parsed_host_port.second);
    handler_impl->socket.connect(address);
    handler_impl->socket.setKeepAlive(true);

    //TODO: get login and password here properly
    user_name = "guest";
    password = "guest";
}

RabbitMQHandler::~RabbitMQHandler()
{
    handler_impl->socket.close();
}

/* Send the data over a socket that is connected with RabbitMQ.
Note that the AMQP library does no buffering by itself. This means that this method
hould always send out all data or do the buffering itself. */
void RabbitMQHandler::onData(AMQP::Connection *connection, const char * data, size_t size)
{
    handler_impl->connection = connection;

    if (!data)
        return;

    handler_impl->socket.sendBytes(data, size);
}

void RabbitMQHandler::onError(AMQP::Connection * /* connection */, const char * message)
{
    LOG_TRACE(log, message);
}


void RabbitMQHandler::onReady(AMQP::Connection * /* connection */)
{
    LOG_TRACE(log, "Connection is ready to use, the RabbitMQ server is ready to receive instructions.");
    handler_impl->connected = true;
}

void RabbitMQHandler::onClosed(AMQP::Connection * /* connection */)
{
    LOG_DEBUG(log, "Connection is closed");
    handler_impl->closed  = true;
}

void RabbitMQHandler::process() 
{
    try
    {
        /* If you notice in your event loop that the socket that is connected with the RabbitMQ server
        becomes readable, you should read out that socket, and pass the received bytes to the AMQP-CPP
        library. This is done by calling the parse() method in the Connection object. */
        while (!handler_impl->closed)
        {
            LOG_TRACE(log, "Waiting for the data to be received");

            if (handler_impl->socket.available() > 0)
            {
                LOG_TRACE(log, "Sending received bytes from socket to the library");

                size_t avail = handler_impl->socket.available();

                if (handler_impl->tmpBuff.size() < avail)
                {
                    handler_impl->tmpBuff.resize(avail, 0);
                }

                handler_impl->socket.receiveBytes(&handler_impl->tmpBuff[0], avail);

                size_t count = 0;
                if (handler_impl->connection)
                {
                    count = handler_impl->connection->parse(handler_impl->tmpBuff.data(), avail);
                }

                if (count != avail)
                {
                    LOG_DEBUG(log, "Sent bytes are not equal to expected");
                    /// TODO: handle this case
                }
                else
                    break;
            }

            if (handler_impl->socket.available() < 0)
            {
                LOG_ERROR(log, "Socket error");
            }

            break; //TODO: Should not break here, should set max retries instead
        }

    } catch (const Poco::Exception& e)
    {
        LOG_ERROR(log, "Poco error: " << e.message());
    }
}

bool RabbitMQHandler::connected() const
{
    return handler_impl->connected;
}

}
