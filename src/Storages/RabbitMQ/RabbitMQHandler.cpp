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
}

void RabbitMQHandler::onError(AMQP::TcpConnection * /* connection */, const char * message) 
{
    LOG_ERROR(log, "Library error report: " << message);
    connection_error = true;
    stop();
}


void RabbitMQHandler::start()
{
    if (connection_error)
        return;

    event_base_dispatch(evbase);
}


bool RabbitMQHandler::startNonBlock()
{
    if (connection_error)
        return true;

    event_base_loop(evbase, EVLOOP_NONBLOCK);
    return false;
}


void RabbitMQHandler::stop()
{
    event_base_loopbreak(evbase);
}


void RabbitMQHandler::free()
{
    event_base_free(evbase);
}

}
