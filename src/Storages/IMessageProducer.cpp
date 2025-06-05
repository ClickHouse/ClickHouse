#include <Storages/IMessageProducer.h>
#include <Common/logger_useful.h>

namespace DB
{

IMessageProducer::IMessageProducer(LoggerPtr log_) : log(log_)
{
}

void AsynchronousMessageProducer::start(const ContextPtr & context)
{
    LOG_TEST(log, "Executing startup");

    try
    {
        initialize();
    }
    catch (...)
    {
        finished = true;
        throw;
    }

    producing_task = context->getSchedulePool().createTask(getProducingTaskName(), [this]
    {
        LOG_TEST(log, "Starting producing task loop");

        scheduled.store(true);
        scheduled.notify_one();

        startProducingTaskLoop();
    });
    producing_task->activateAndSchedule();
}

void AsynchronousMessageProducer::finish()
{
    /// We should execute finish logic only once.
    if (finished.exchange(true))
        return;

    LOG_TEST(log, "Executing shutdown");

    /// It is possible that the task with a producer loop haven't been started yet
    /// while we have non empty payloads queue.
    /// If we deactivate it here, the messages will never be sent,
    /// as the producer loop will never start.
    scheduled.wait(false);
    /// Tell the task that it should shutdown, but not immediately,
    /// it will finish executing current tasks nevertheless.
    stopProducingTask();
    /// Wait for the producer task to finish.
    producing_task->deactivate();
    finishImpl();
}


}
