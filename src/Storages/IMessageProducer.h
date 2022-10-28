#pragma once

#pragma once

#include <Processors/Sinks/SinkToStorage.h>
#include <Interpreters/Context.h>
#include <Core/BackgroundSchedulePool.h>

namespace DB
{

/// Interface for producing messages in streaming storages.
/// It's used in MessageQueueSink.
class IMessageProducer
{
public:
    /// Do some preparations.
    virtual void start(const ContextPtr & context) = 0;

    /// Produce single message.
    virtual void produce(const String & message, size_t rows_in_message, const Columns & columns, size_t last_row) = 0;

    /// Finalize producer.
    virtual void finish() = 0;

    virtual ~IMessageProducer() = default;
};

/// Implements interface for concurrent message producing.
class ConcurrentMessageProducer : public IMessageProducer
{
public:
    /// Create and schedule task in BackgroundSchedulePool that will produce messages.
    void start(const ContextPtr & context) override;

    /// Stop producing task, wait for ot to finish and finalize.
    void finish() override;

    /// In this method producer should not do any hard work and send message
    /// to producing task, for example, by using ConcurrentBoundedQueue.
    void produce(const String & message, size_t rows_in_message, const Columns & columns, size_t last_row) override = 0;

protected:
    /// Do some initialization before scheduling producing task.
    virtual void initialize() {}
    /// Tell producer to finish all work and stop producing task
    virtual void stopProducingTask() = 0;
    /// Do some finalization after producing task is stopped.
    virtual void finishImpl() {}

    virtual String getProducingTaskName() const = 0;
    /// Method that is called inside producing task, all producing wokr should be done here.
    virtual void producingTask() = 0;

private:
    /// Flag, indicated that finish() method was called.
    /// It's used to prevent doing finish logic more than once.
    std::atomic<bool> finished = false;
    /// Flag, indicated that producing task was finished.
    /// It's used to wait until producing task is finished.
    std::atomic<bool> task_finished = false;

    BackgroundSchedulePool::TaskHolder producing_task;
};


}
