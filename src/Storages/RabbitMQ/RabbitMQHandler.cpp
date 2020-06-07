#include <common/logger_useful.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>

namespace DB
{

enum
{
    Lock_timeout = 50,
    Loop_stop_timeout = 200
};


RabbitMQHandler::RabbitMQHandler(event_base * evbase_, Poco::Logger * log_) :
    LibEventHandler(evbase_),
    evbase(evbase_),
    log(log_)
{
    tv.tv_sec = 0;
    tv.tv_usec = Loop_stop_timeout;
}


void RabbitMQHandler::onError(AMQP::TcpConnection * connection, const char * message)
{
    LOG_ERROR(log, "Library error report: {}", message);

    if (!connection->usable() || !connection->ready())
    {
        LOG_ERROR(log, "Connection lost completely");
    }

    stop();
}


void RabbitMQHandler::startConsumerLoop(std::atomic<bool> & check_param, std::atomic<bool> & loop_started)
{
    /* The object of this class is shared between concurrent consumers (who share the same connection == share the same
     * event loop). But the loop should not be attempted to start if it is already running. 
     */
    if (mutex_before_event_loop.try_lock_for(std::chrono::milliseconds(Lock_timeout)))
    {
        /* The callback, which changes this variable, could have already been activated by another thread while we waited
         * for the mutex to unlock (as it runs all active events on the connection). This means that there is no need to
         * start event loop again.
         */
        if (!check_param)
        {
            loop_started = true;
            event_base_loop(evbase, EVLOOP_NONBLOCK);
        }

        mutex_before_event_loop.unlock();
    }
}


void RabbitMQHandler::startProducerLoop()
{
    event_base_loop(evbase, EVLOOP_NONBLOCK);
}


void RabbitMQHandler::stop()
{
    if (mutex_before_loop_stop.try_lock_for(std::chrono::milliseconds(0)))
    {
        event_base_loopbreak(evbase);
        mutex_before_loop_stop.unlock();
    }
}


void RabbitMQHandler::stopWithTimeout()
{
    if (mutex_before_loop_stop.try_lock_for(std::chrono::milliseconds(0)))
    {
        event_base_loopexit(evbase, &tv);
        mutex_before_loop_stop.unlock();
    }
}

}
