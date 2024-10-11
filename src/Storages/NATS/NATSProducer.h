#pragma once

#include <atomic>
#include <list>
#include <mutex>
#include <Columns/IColumn.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/Names.h>
#include <Storages/NATS/NATSConnection.h>
#include <Storages/NATS/NATSHandler.h>
#include <Storages/IMessageProducer.h>
#include <Common/ConcurrentBoundedQueue.h>

namespace DB
{

class NATSProducer : public AsynchronousMessageProducer
{
public:
    NATSProducer(const NATSConfiguration & configuration_, BackgroundSchedulePool & broker_schedule_pool_, const String & subject_, LoggerPtr log_);

    void produce(const String & message, size_t rows_in_message, const Columns & columns, size_t last_row) override;

private:
    String getProducingTaskName() const override { return "NatsProducingTask"; }

    void initialize() override;

    void stopProducingTask() override;
    void finishImpl() override;

    void startProducingTaskLoop() override;

    void publish();

    NATSConfiguration configuration;

    NATSHandler event_handler;
    BackgroundSchedulePool::TaskHolder looping_task;

    NATSConnectionPtr connection;
    const String subject;

    /* payloads.queue:
     *      - payloads are pushed to queue in countRow and popped by another thread in writingFunc, each payload gets into queue only once
     */
    ConcurrentBoundedQueue<String> payloads;
};

}
