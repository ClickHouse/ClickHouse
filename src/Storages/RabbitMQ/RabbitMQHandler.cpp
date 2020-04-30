#include <Poco/Net/StreamSocket.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>
#include <common/logger_useful.h>


namespace DB
{


RabbitMQHandler::RabbitMQHandler(event_base * evbase_, Poco::Logger * log_) :
    LibEventHandler(evbase_),
    evbase(evbase_),
    log(log_)
{
    user_name = "root";
    password = "clickhouse";
}

void RabbitMQHandler::onError(AMQP::TcpConnection * /* connection */, const char * message) 
{
    LOG_ERROR(log, "Library error report: " << message);
    event_base_loopbreak(evbase);
}


void RabbitMQHandler::onClosed(AMQP::TcpConnection * /* connection */)
{
    LOG_DEBUG(log, "Connection is closed successfully");
} 


void RabbitMQHandler::start()
{
    LOG_DEBUG(log, "Event loop started.");
    event_base_dispatch(evbase);
}

void RabbitMQHandler::startNonBlock()
{
    LOG_DEBUG(log, "Event timeout loop started.");
    event_base_loop(evbase, EVLOOP_NONBLOCK);
}


void RabbitMQHandler::stop()
{
    LOG_DEBUG(log, "Event loop stopped.");
    event_base_loopbreak(evbase);
}

void RabbitMQHandler::free()
{
    event_base_free(evbase);
}

}
