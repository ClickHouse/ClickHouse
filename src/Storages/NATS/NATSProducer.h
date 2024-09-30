#pragma once

#include <atomic>
#include <list>
#include <mutex>
#include <Columns/IColumn.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/Names.h>
#include <Storages/NATS/NATSConnection.h>
#include <Storages/IMessageProducer.h>
#include <Common/ConcurrentBoundedQueue.h>

namespace DB
{

class NATSProducer : public AsynchronousMessageProducer
{
    using ReconnectCallback = std::function<void (NATSConnectionPtr)>;

public:
    NATSProducer(
        const NATSConfiguration & configuration_,
        NATSOptionsPtr options_,
        const String & subject_,
        LoggerPtr log_,
        ReconnectCallback reconnect_callback_);
    ~NATSProducer() override;

    void produce(const String & message, size_t rows_in_message, const Columns & columns, size_t last_row) override;

private:
    String getProducingTaskName() const override { return "NatsProducingTask"; }

    void initialize() override;
    void stopProducingTask() override;
    void finishImpl() override;

    void startProducingTaskLoop() override;

    void publish();

    NATSConnectionPtr connection;
    const String subject;

    ReconnectCallback reconnect_callback;

    /* payloads.queue:
     *      - payloads are pushed to queue in countRow and popped by another thread in writingFunc, each payload gets into queue only once
     */
    ConcurrentBoundedQueue<String> payloads;
};

}
