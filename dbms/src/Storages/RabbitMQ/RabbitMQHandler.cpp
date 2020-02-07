#include <Poco/Net/StreamSocket.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>
#include <common/logger_useful.h>

namespace DB
{

/// to connect to rabbitmq server
RabbitMQHandler::RabbitMQHandler(const std::pair<std::string, UInt16> & parsed_host_port, Poco::Logger * log_) :
        log(log_),
        handler_impl(new RabbitMQHandlerImpl)
{
    const Poco::Net::SocketAddress address(parsed_host_port.first, parsed_host_port.second);
    handler_impl->socket.connect(address);
    handler_impl->socket.setKeepAlive(true);

    user_name = "guest"; // TODO: get user_name here properly
    password = "guest"; // TODO: get password here properly
}

RabbitMQHandler::~RabbitMQHandler()
{
    handler_impl->socket.close();
}

/* Send the data over a socket that is connected with RabbitMQ.
Note that the AMQP library does no buffering by itself. This means that this method
hould always send out all data or do the buffering itself. */
void RabbitMQHandler::onData(AMQP::Connection *connection, const char *data, size_t size)
{
    handler_impl->connection = connection;

    size_t avail = handler_impl->outputBuffer.available();
    size_t written = size < avail ? size : avail;

    handler_impl->outputBuffer.write(data, written);

    if (written != size)
    {
        sendDataToRabbitMQ();
        handler_impl->outputBuffer.write(data, size - written);
    }
}

void RabbitMQHandler::onError(AMQP::Connection * /* connection */, const char * message)
{
    LOG_TRACE(log, message);
}

void RabbitMQHandler::onReady(AMQP::Connection *)
{
    LOG_TRACE(log, "Connection is ready to use, the RabbitMQ server is ready to receive instructions.");
    handler_impl->connected = true;
}

void RabbitMQHandler::onClosed(AMQP::Connection * /*connection*/)
{
    handler_impl->closed  = true;
}

void RabbitMQHandler::process()
{
    try
    {
        /* If you notice in your event loop that the socket that is connected with the RabbitMQ server
        becomes readable, you should read out that socket, and pass the received bytes to the AMQP-CPP
        library. This is done by calling the parse() method in the Connection object. */
        while (handler_impl->readable && !handler_impl->closed)
        {
            if (handler_impl->socket.available() > 0)
            {
                size_t avail = handler_impl->socket.available();
                if (handler_impl->tmpBuff.size() < avail)
                {
                    handler_impl->tmpBuff.resize(avail, 0);
                }

                handler_impl->socket.receiveBytes(&handler_impl->tmpBuff[0], avail);
                handler_impl->inputBuffer.write(handler_impl->tmpBuff.data(), avail);
            }

            if (handler_impl->socket.available() < 0)
            {
                LOG_TRACE(log, "Socket error!");
            }

            if (handler_impl->connection && handler_impl->inputBuffer.count())
            {
                size_t count = handler_impl->connection->parse(handler_impl->inputBuffer.buffer().begin(),
                                                         handler_impl->inputBuffer.count());

                if (count == handler_impl->inputBuffer.count())
                {
                    handler_impl->inputBuffer.set(nullptr, 0);
                }
                else if (count > 0)
                {
                    handler_impl->inputBuffer.set(handler_impl->inputBuffer.buffer().begin() + count, count);
                }
            }

            sendDataToRabbitMQ();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        if (!handler_impl->readable && handler_impl->outputBuffer.count())
        {
            sendDataToRabbitMQ();
        }

    } catch (const Poco::Exception& e)
    {
        LOG_TRACE(log, "Poco error: " << e.message());
    }
}

void RabbitMQHandler::sendDataToRabbitMQ()
{
    if (handler_impl->outputBuffer.count())
    {
        handler_impl->socket.sendBytes(handler_impl->outputBuffer.buffer().begin(), handler_impl->outputBuffer.count());
        handler_impl->outputBuffer.set(nullptr, 0);
    }
}

bool RabbitMQHandler::connected() const
{
    return handler_impl->connected;
}

void RabbitMQHandler::onProduced()
{
    handler_impl->readable = true;
}

void RabbitMQHandler::onProcessed()
{
    handler_impl->readable = false;
}

}
