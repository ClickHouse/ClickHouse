#include <common/logger_useful.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>

namespace DB
{

RabbitMQHandler::RabbitMQHandler(event_base * evbase_, Poco::Logger * log_) :
    LibEventHandler(evbase_),
    evbase(evbase_),
    log(log_)
{
}


void RabbitMQHandler::onError(AMQP::TcpConnection * /*connection*/, const char * message) 
{
    LOG_ERROR(log, "Library error report: " << message);
    stop();
}


void RabbitMQHandler::startNonBlock()
{
    event_base_loop(evbase, EVLOOP_NONBLOCK); 
}

void RabbitMQHandler::stop()
{
    event_base_loopbreak(evbase);
}

}
