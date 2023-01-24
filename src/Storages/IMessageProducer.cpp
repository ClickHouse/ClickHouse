#include <Storages/IMessageProducer.h>
#include <Common/logger_useful.h>

namespace DB
{

void AsynchronousMessageProducer::start(const ContextPtr & context)
{
    initialize();
    producing_task = context->getSchedulePool().createTask(getProducingTaskName(), [this]
    {
        startProducingTaskLoop();
    });
    producing_task->activateAndSchedule();
}

void AsynchronousMessageProducer::finish()
{
    /// We should execute finish logic only once.
    if (finished.exchange(true))
        return;

    stopProducingTask();
    /// Deactivate producing task and wait until it's finished.
    producing_task->deactivate();
    finishImpl();
}


}
