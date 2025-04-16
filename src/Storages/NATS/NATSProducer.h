#pragma once

#include <atomic>
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
    NATSProducer(NATSConnectionPtr connection_, const String & subject_, std::atomic<bool> & shutdown_called_, LoggerPtr log_);

    void produce(const String & message, size_t rows_in_message, const Columns & columns, size_t last_row) override;
    void cancel() noexcept override;

private:
    String getProducingTaskName() const override { return "NatsProducingTask"; }

    void stopProducingTask() override;
    void finishImpl() override;

    void startProducingTaskLoop() override;

    void publish();

    NATSConnectionPtr connection;
    const String subject;

    std::atomic<bool> & shutdown_called;

    /* payloads.queue:
     *      - payloads are pushed to queue in countRow and popped by another thread in writingFunc, each payload gets into queue only once
     */
    ConcurrentBoundedQueue<String> payloads;
};

}
