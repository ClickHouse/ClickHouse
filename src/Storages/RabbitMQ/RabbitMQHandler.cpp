#include <Poco/Net/StreamSocket.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>
#include <common/logger_useful.h>


namespace DB
{
RabbitMQHandler::RabbitMQHandler(event_base * event, Poco::Logger * log_)
    : AMQP::LibEventHandler(event), log(log_), handler_impl(std::make_shared<ConnectionImpl>())
{
    //TODO: get credentials here properly
    user_name = "root";
    password = "clickhouse";
    LOG_DEBUG(log, "Handler created");
}

RabbitMQHandler::~RabbitMQHandler()
{
}

    //void RabbitMQHandler::process()
    //{
    //    try
    //    {
    //        /* If you notice in your event loop that the socket that is connected with the RabbitMQ server
    //        becomes readable, you should read out that socket, and pass the received bytes to the AMQP-CPP
    //        library. This is done by calling the parse() method in the Connection object. */
    //        while (!handler_impl->closed)
    //        {
    //            LOG_DEBUG(log, "Waiting for the data to be received");
    //
    //            if (handler_impl->socket.available() > 0)
    //            {
    //                LOG_DEBUG(log, "Sending received bytes from socket to the library");
    //
    //                size_t avail = handler_impl->socket.available();
    //
    //                if (handler_impl->tmpBuff.size() < avail)
    //                {
    //                    handler_impl->tmpBuff.resize(avail, 0);
    //                }
    //
    //                handler_impl->socket.receiveBytes(&handler_impl->tmpBuff[0], avail);
    //
    //                size_t count = 0;
    //                if (handler_impl->connection)
    //                {
    //                    count = handler_impl->connection->parse(handler_impl->tmpBuff.data(), avail);
    //                }
    //
    //                if (count != avail)
    //                {
    //                    LOG_DEBUG(log, "Sent bytes are not equal to expected");
    //                    /// TODO: handle this case
    //                }
    //                else
    //                    break;
    //            }
    //
    //            if (handler_impl->socket.available() < 0)
    //            {
    //                LOG_ERROR(log, "Socket error");
    //            }
    //
    //            break; //TODO: Should not break here, should set max retries instead
    //        }
    //
    //    } catch (const Poco::Exception& e)
    //    {
    //        LOG_ERROR(log, "Poco error: " << e.message());
    //    }
    //}

bool RabbitMQHandler::connected() const
{
    return handler_impl->connected;
}

}
