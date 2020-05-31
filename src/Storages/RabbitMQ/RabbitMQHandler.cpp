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


void RabbitMQHandler::onError(AMQP::TcpConnection * /* connection */, const char * message) 
{
    LOG_ERROR(log, "Library error report: {}", message);
    stop();
}


void RabbitMQHandler::start(std::atomic<bool> & check_param)
{
    /* The object of this class is shared between concurrent consumers, who call this method repeatedly at the same time.
     * But the loop should not be attempted to start if it is already running. Also note that the loop is blocking to
     * the thread that has started it.
     */
    std::lock_guard lock(mutex);

    /* The callback, which changes this variable, could have already been activated by another thread while we waited for the
     * mutex to unlock (as it runs all active events on the connection). This means that there is no need to start event loop again.
     */
    if (check_param)
        return;

    event_base_loop(evbase, EVLOOP_NONBLOCK); 
}

void RabbitMQHandler::stop()
{
    std::lock_guard lock(mutex);
    event_base_loopbreak(evbase);
}

}
