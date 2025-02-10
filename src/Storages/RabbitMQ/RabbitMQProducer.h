#pragma once

#include <Columns/IColumn.h>
#include <list>
#include <mutex>
#include <atomic>
#include <amqpcpp.h>
#include <Storages/RabbitMQ/RabbitMQConnection.h>
#include <Storages/IMessageProducer.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Core/Names.h>

namespace DB
{

class RabbitMQProducer : public AsynchronousMessageProducer
{
public:
    RabbitMQProducer(
        const RabbitMQConfiguration & configuration_,
        const Names & routing_keys_,
        const String & exchange_name_,
        AMQP::ExchangeType exchange_type_,
        size_t channel_id_base_,
        bool persistent_,
        std::atomic<bool> & shutdown_called_,
        LoggerPtr log_);

    void produce(const String & message, size_t rows_in_message, const Columns & columns, size_t last_row) override;

private:
    String getProducingTaskName() const override { return "RabbitMQProducingTask"; }

    struct Payload
    {
        String message;
        UInt64 id;
    };

    using Payloads = ConcurrentBoundedQueue<Payload>;

    void initialize() override;
    void stopProducingTask() override;
    void finishImpl() override;

    int iterateEventLoop();
    void startProducingTaskLoop() override;
    void setupChannel();
    void removeRecord(UInt64 received_delivery_tag, bool multiple, bool republish);
    void publish(Payloads & messages, bool republishing);

    RabbitMQConnection connection;

    const Names routing_keys;
    const String exchange_name;
    AMQP::ExchangeType exchange_type;
    const String channel_id_base; /// Serial number of current producer buffer
    const bool persistent;

    /* false: when shutdown is called; needed because table might be dropped before all acks are received
     * true: in all other cases
     */
    std::atomic<bool> & shutdown_called;

    AMQP::Table key_arguments;

    std::unique_ptr<AMQP::TcpChannel> producer_channel;
    bool producer_ready = false;

    /// Channel errors lead to channel closure, need to count number of recreated channels to update channel id
    UInt64 channel_id_counter = 0;

    /// channel id which contains id of current producer buffer and serial number of recreated channel in this buffer
    String channel_id;

    /* payloads.queue:
     *      - payloads are pushed to queue in countRow and popped by another thread in writingFunc, each payload gets into queue only once
     * returned.queue:
     *      - payloads are pushed to queue:
     *           1) inside channel->onError() callback if channel becomes unusable and the record of pending acknowledgements from server
     *              is non-empty.
     *           2) inside removeRecord() if received nack() - negative acknowledgement from the server that message failed to be written
     *              to disk or it was unable to reach the queue.
     *      - payloads are popped from the queue once republished
     */
    Payloads payloads, returned;

    /* Counter of current delivery on a current channel. Delivery tags are scoped per channel. The server attaches a delivery tag for each
     * published message - a serial number of delivery on current channel. Delivery tag is a way of server to notify publisher if it was
     * able or unable to process delivery, i.e. it sends back a response with a corresponding delivery tag.
     */
    UInt64 delivery_tag = 0;

    /* false: message delivery successfully ended: publisher received confirm from server that all published
     *  1) persistent messages were written to disk
     *  2) non-persistent messages reached the queue
     * true: continue to process deliveries and returned messages
     */
//    bool wait_all = true;

    /// Needed to fill messageID property
    UInt64 payload_counter = 0;

    /// Record of pending acknowledgements from the server; its size never exceeds size of returned.queue
    std::map<UInt64, Payload> delivery_record;
};

}
