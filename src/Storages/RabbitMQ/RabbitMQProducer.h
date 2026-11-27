#pragma once

#include <Columns/IColumn_fwd.h>
#include <Storages/RabbitMQ/RabbitMQPublisherPool.h>
#include <Storages/IMessageProducer.h>

#include <atomic>

namespace DB
{

class RabbitMQProducer : public AsynchronousMessageProducer
{
public:
    RabbitMQProducer(
        RabbitMQPublisherPoolPtr publisher_pool_,
        std::atomic<bool> & shutdown_called_,
        LoggerPtr log_);

    void produce(const String & message, size_t rows_in_message, const Columns & columns, size_t last_row) override;
    void cancel() noexcept override;

private:
    String getProducingTaskName() const override { return "RabbitMQProducingTask"; }

    using Payload = RabbitMQPublisher::Payload;
    using Payloads = RabbitMQPublisher::Payloads;

    void initialize() override;
    void stopProducingTask() override;
    void finishImpl() override;

    void startProducingTaskLoop() override;

    RabbitMQPublisherPoolPtr publisher_pool;

    RabbitMQPublisherPtr publisher;

    bool can_reuse_publisher = false;

    /* false: when shutdown is called; needed because table might be dropped before all acks are received
     * true: in all other cases
     */
    std::atomic<bool> & shutdown_called;

    /* payloads.queue:
     *      - payloads are pushed to queue in countRow and popped by another thread in writingFunc, each payload gets into queue only once
     */
    Payloads payloads;

    /// Needed to fill messageID property
    UInt64 payload_counter = 0;
};

}
