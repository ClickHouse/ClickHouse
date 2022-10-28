#include <Storages/IMessageProducer.h>
#include <Core/BackgroundSchedulePool.h>
#include <Common/logger_useful.h>

namespace DB
{

void ConcurrentMessageProducer::start(const ContextPtr & context)
{
    initialize();
    producing_task = context->getSchedulePool().createTask(getProducingTaskName(), [this]
    {
        producingTask();
        /// Notify that producing task is finished.
        task_finished.store(true);
        task_finished.notify_all();
    });
    producing_task->activateAndSchedule();
}

void ConcurrentMessageProducer::finish()
{
    LOG_DEBUG(&Poco::Logger::get("ConcurrentMessageProducer"), "finish");

    /// We should execute finish logic only once.
    if (finished.exchange(true))
        return;

    stopProducingTask();
    /// Wait until producing task is finished.
    task_finished.wait(false);
    producing_task->deactivate();
    finishImpl();
}


}
