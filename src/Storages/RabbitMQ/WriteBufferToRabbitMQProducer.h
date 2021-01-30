#pragma once

#include <IO/WriteBuffer.h>
#include <Columns/IColumn.h>
#include <list>
#include <mutex>
#include <atomic>
#include <amqpcpp.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/Names.h>

namespace DB
{

class WriteBufferToRabbitMQProducer : public WriteBuffer
{
public:
    WriteBufferToRabbitMQProducer(
            std::pair<String, UInt16> & parsed_address_,
            const Context & global_context,
            const std::pair<String, String> & login_password_,
            const Names & routing_keys_,
            const String & exchange_name_,
            const AMQP::ExchangeType exchange_type_,
            const size_t channel_id_,
            const bool persistent_,
            std::atomic<bool> & wait_confirm_,
            Poco::Logger * log_,
            std::optional<char> delimiter,
            size_t rows_per_message,
            size_t chunk_size_
    );

    ~WriteBufferToRabbitMQProducer() override;

    void countRow();
    void activateWriting() { writing_task->activateAndSchedule(); }
    void updateMaxWait() { wait_num.store(payload_counter); }

private:
    void nextImpl() override;
    void iterateEventLoop();
    void writingFunc();
    bool setupConnection(bool reconnecting);
    void setupChannel();
    void removeRecord(UInt64 received_delivery_tag, bool multiple, bool republish);
    void publish(ConcurrentBoundedQueue<std::pair<UInt64, String>> & message, bool republishing);

    std::pair<String, UInt16> parsed_address;
    const std::pair<String, String> login_password;
    const Names routing_keys;
    const String exchange_name;
    AMQP::ExchangeType exchange_type;
    const String channel_id_base; /// Serial number of current producer buffer
    const bool persistent;

    /* false: when shutdown is called; needed because table might be dropped before all acks are received
     * true: in all other cases
     */
    std::atomic<bool> & wait_confirm;

    AMQP::Table key_arguments;
    BackgroundSchedulePool::TaskHolder writing_task;

    std::unique_ptr<uv_loop_t> loop;
    std::unique_ptr<RabbitMQHandler> event_handler;
    std::unique_ptr<AMQP::TcpConnection> connection;
    std::unique_ptr<AMQP::TcpChannel> producer_channel;

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
    ConcurrentBoundedQueue<std::pair<UInt64, String>> payloads, returned;

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
    bool wait_all = true;

    /* false: until writeSuffix is called
     * true: means payloads.queue will not grow anymore
     */
    std::atomic<UInt64> wait_num = 0;

    /// Needed to fill messageID property
    UInt64 payload_counter = 0;

    /// Record of pending acknowledgements from the server; its size never exceeds size of returned.queue
    std::map<UInt64, std::pair<UInt64, String>> delivery_record;

    Poco::Logger * log;
    const std::optional<char> delim;
    const size_t max_rows;
    const size_t chunk_size;
    size_t rows = 0;
    std::list<std::string> chunks;
};

}
