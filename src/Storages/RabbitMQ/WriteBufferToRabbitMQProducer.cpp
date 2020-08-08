#include <Storages/RabbitMQ/WriteBufferToRabbitMQProducer.h>
#include "Core/Block.h"
#include "Columns/ColumnString.h"
#include "Columns/ColumnsNumber.h"
#include <common/logger_useful.h>
#include <amqpcpp.h>
#include <uv.h>
#include <chrono>
#include <thread>
#include <atomic>


namespace DB
{

static const auto CONNECT_SLEEP = 200;
static const auto RETRIES_MAX = 20;
static const auto BATCH = 512;

WriteBufferToRabbitMQProducer::WriteBufferToRabbitMQProducer(
        std::pair<String, UInt16> & parsed_address_,
        Context & global_context,
        const std::pair<String, String> & login_password_,
        const Names & routing_keys_,
        const String & exchange_name_,
        const AMQP::ExchangeType exchange_type_,
        const size_t channel_id_,
        const bool use_tx_,
        const bool persistent_,
        Poco::Logger * log_,
        std::optional<char> delimiter,
        size_t rows_per_message,
        size_t chunk_size_)
        : WriteBuffer(nullptr, 0)
        , parsed_address(parsed_address_)
        , login_password(login_password_)
        , routing_keys(routing_keys_)
        , exchange_name(exchange_name_)
        , exchange_type(exchange_type_)
        , channel_id(std::to_string(channel_id_))
        , use_tx(use_tx_)
        , persistent(persistent_)
        , payloads(BATCH)
        , returned(BATCH << 6)
        , log(log_)
        , delim(delimiter)
        , max_rows(rows_per_message)
        , chunk_size(chunk_size_)
{

    loop = std::make_unique<uv_loop_t>();
    uv_loop_init(loop.get());
    event_handler = std::make_unique<RabbitMQHandler>(loop.get(), log);

    /* New coonection for each producer buffer because cannot publish from different threads with the same connection.
     * (https://github.com/CopernicaMarketingSoftware/AMQP-CPP/issues/128#issuecomment-300780086)
     */
    if (setupConnection(false))
        setupChannel();

    writing_task = global_context.getSchedulePool().createTask("RabbitMQWritingTask", [this]{ writingFunc(); });
    writing_task->deactivate();

    if (exchange_type == AMQP::ExchangeType::headers)
    {
        for (const auto & header : routing_keys)
        {
            std::vector<String> matching;
            boost::split(matching, header, [](char c){ return c == '='; });
            key_arguments[matching[0]] = matching[1];
        }
    }
}


WriteBufferToRabbitMQProducer::~WriteBufferToRabbitMQProducer()
{
    writing_task->deactivate();
    connection->close();
    assert(rows == 0 && chunks.empty());
}


void WriteBufferToRabbitMQProducer::countRow()
{
    if (++rows % max_rows == 0)
    {
        const std::string & last_chunk = chunks.back();
        size_t last_chunk_size = offset();

        if (delim && last_chunk[last_chunk_size - 1] == delim)
            --last_chunk_size;

        std::string payload;
        payload.reserve((chunks.size() - 1) * chunk_size + last_chunk_size);

        for (auto i = chunks.begin(), e = --chunks.end(); i != e; ++i)
            payload.append(*i);

        payload.append(last_chunk, 0, last_chunk_size);

        rows = 0;
        chunks.clear();
        set(nullptr, 0);

        if (!use_tx)
        {
            /// "publisher confirms" will be used, this is default.
            ++payload_counter;
            payloads.push(std::make_pair(payload_counter, payload));
        }
        else
        {
            /// means channel->startTransaction() was called, not default, enabled only with table setting.
            publish(payload);
        }
    }
}


bool WriteBufferToRabbitMQProducer::setupConnection(bool reconnecting)
{
    size_t cnt_retries = 0;
    if (reconnecting)
    {
        /* connection->close() is called in onError() method (called by the AMQP library when a fatal error occurs on the connection)
         * inside event_handler, but it is not closed immediately (firstly, all pending operations are completed, and then an AMQP
         * closing-handshake is  performed). But cannot open a new connection untill previous one is properly closed).
         */
        while (!connection->closed() && ++cnt_retries != (RETRIES_MAX >> 1))
            event_handler->iterateLoop();
        if (!connection->closed())
            connection->close(true);
    }

    LOG_TRACE(log, "Trying to set up connection");
    connection = std::make_unique<AMQP::TcpConnection>(event_handler.get(),
            AMQP::Address(parsed_address.first, parsed_address.second, AMQP::Login(login_password.first, login_password.second), "/"));

    cnt_retries = 0;
    while (!connection->ready() && ++cnt_retries != RETRIES_MAX)
    {
        event_handler->iterateLoop();
        std::this_thread::sleep_for(std::chrono::milliseconds(CONNECT_SLEEP));
    }

    return connection->ready();
}


void WriteBufferToRabbitMQProducer::setupChannel()
{
    producer_channel = std::make_unique<AMQP::TcpChannel>(connection.get());

    producer_channel->onError([&](const char * message)
    {
        LOG_ERROR(log, "Producer error: {}", message);

        /// Channel is not usable anymore. (https://github.com/CopernicaMarketingSoftware/AMQP-CPP/issues/36#issuecomment-125112236)
        producer_channel->close();

        /// Records that have not received ack/nack from server before channel closure.
        for (const auto & record : delivery_record)
            returned.tryPush(record.second);

        LOG_DEBUG(log, "Currently {} messages have not been confirmed yet, {} waiting to be published, {} will be republished",
                delivery_record.size(), payloads.size(), returned.size());

        /// Delivery tags are scoped per channel.
        delivery_record.clear();
        delivery_tag = 0;
    });

    producer_channel->onReady([&]()
    {
        LOG_DEBUG(log, "Producer channel is ready");

        if (use_tx)
        {
            producer_channel->startTransaction();
        }
        else
        {
            /* if persistent == true, onAck is received when message is persisted to disk or when it is consumed on every queue. If fails,
             * onNack() is received. If persistent == false, message is confirmed the moment it is enqueued. First option is two times
             * slower than the second, so default is second and the first is turned on in table setting.
             *
             * "Publisher confirms" are implemented similar to strategy#3 here https://www.rabbitmq.com/tutorials/tutorial-seven-java.html
             */
            producer_channel->confirmSelect()
            .onAck([&](uint64_t acked_delivery_tag, bool multiple)
            {
                removeConfirmed(acked_delivery_tag, multiple, false);
            })
            .onNack([&](uint64_t nacked_delivery_tag, bool multiple, bool /* requeue */)
            {
                removeConfirmed(nacked_delivery_tag, multiple, true);
            });
        }
    });
}


void WriteBufferToRabbitMQProducer::removeConfirmed(UInt64 received_delivery_tag, bool multiple, bool republish)
{
    auto record_iter = delivery_record.find(received_delivery_tag);

    if (record_iter != delivery_record.end())
    {
        if (multiple)
        {
            /// If multiple is true, then all delivery tags up to and including current are confirmed (with ack or nack).
            ++record_iter;

            if (republish)
                for (auto record = delivery_record.begin(); record != record_iter; ++record)
                    returned.tryPush(record->second);

            /// Delete the records even in case when republished because new delivery tags will be assigned by the server.
            delivery_record.erase(delivery_record.begin(), record_iter);

            //LOG_DEBUG(log, "Confirmed all delivery tags up to {}", received_delivery_tag);
        }
        else
        {
            if (republish)
                returned.tryPush(record_iter->second);

            delivery_record.erase(record_iter);

            //LOG_DEBUG(log, "Confirmed delivery tag {}", received_delivery_tag);
        }
    }
    /// else is theoretically not possible
}


void WriteBufferToRabbitMQProducer::publish(ConcurrentBoundedQueue<std::pair<UInt64, String>> & messages, bool republishing)
{
    std::pair<UInt64, String> payload;
    while (!messages.empty() && producer_channel->usable())
    {
        messages.pop(payload);
        AMQP::Envelope envelope(payload.second.data(), payload.second.size());

        /// if headers exchange - routing keys are added here via headers, else - it is just empty.
        AMQP::Table message_settings = key_arguments;

        /* There is the case when connection is lost in the period after some messages were published and before ack/nack was sent by the
         * server, then it means that publisher will never now whether those messages were delivered or not, and therefore those records
         * that received no ack/nack before connection loss will be republished (see onError() callback), so there might be duplicates. To
         * let consumer know that received message might be a possible duplicate - a "republished" field is added to message metadata.
         */
        message_settings["republished"] = std::to_string(republishing);

        envelope.setHeaders(message_settings);

        /* Adding here a message_id property to message metadata.
         * (https://stackoverflow.com/questions/59384305/rabbitmq-how-to-handle-unwanted-duplicate-un-ack-message-after-connection-lost)
         */
        envelope.setMessageID(channel_id + "-" + std::to_string(payload.first));

        /// Delivery mode is 1 or 2. 1 is default. 2 makes a message durable, but makes performance 1.5-2 times worse.
        if (persistent)
            envelope.setDeliveryMode(2);

        if (exchange_type == AMQP::ExchangeType::consistent_hash)
        {
            producer_channel->publish(exchange_name, std::to_string(delivery_tag), envelope);
        }
        else if (exchange_type == AMQP::ExchangeType::headers)
        {
            producer_channel->publish(exchange_name, "", envelope);
        }
        else
        {
            producer_channel->publish(exchange_name, routing_keys[0], envelope);
        }

        ++delivery_tag;
        delivery_record.insert(delivery_record.end(), {delivery_tag, payload});

        /// Need to break at some point to let event loop run, because no publishing actually happend before looping.
        if (delivery_tag % BATCH == 0)
            break;
    }

    iterateEventLoop();
}


void WriteBufferToRabbitMQProducer::writingFunc()
{
    if (use_tx)
        return;

    while (!payloads.empty() || wait_all)
    {
        /// This check is to make sure that delivery_record.size() is never bigger than returned.size()
        if (delivery_record.size() < (BATCH << 6))
        {
            /* Publish main paylods only when there are no returned messages. This way it is ensured that returned.queue never grows too
             * big and returned messages are republished as fast as possible. Also payloads.queue is fixed size and push attemt would
             * block thread in countRow() once there is no space - that is intended.
             */
            if (!returned.empty() && producer_channel->usable())
                publish(returned, true);
            else if (!payloads.empty() && producer_channel->usable())
                publish(payloads, false);
        }

        iterateEventLoop();

        if (wait_num.load() && delivery_record.empty() && payloads.empty() && returned.empty())
            wait_all = false;
        else if ((!producer_channel->usable() && connection->usable()) || (!connection->usable() && setupConnection(true)))
            setupChannel();
    }

    LOG_DEBUG(log, "Processing ended");
}


/* This publish is for the case when transaction is delcared on the channel with channel->startTransaction(). Here only publish
 * once payload is available and then commitTransaction() is called, where a needed event loop will run.
 */
void WriteBufferToRabbitMQProducer::publish(const String & payload)
{
    AMQP::Envelope envelope(payload.data(), payload.size());

    if (persistent)
        envelope.setDeliveryMode(2);

    if (exchange_type == AMQP::ExchangeType::consistent_hash)
    {
        producer_channel->publish(exchange_name, std::to_string(delivery_tag), envelope);
    }
    else if (exchange_type == AMQP::ExchangeType::headers)
    {
        producer_channel->publish(exchange_name, "", envelope);
    }
    else
    {
        producer_channel->publish(exchange_name, routing_keys[0], envelope);
    }
}


void WriteBufferToRabbitMQProducer::commit()
{
    /* Actually have not yet found any information about how is it supposed work once any error occurs with a channel, because any channel
     * error closes this channel and any operation on a closed channel will fail (but transaction is unique to channel).
     * RabbitMQ transactions seem not trust-worthy at all - see https://www.rabbitmq.com/semantics.html. Seems like its best to always
     * use "publisher confirms" rather than transactions (and by default it is so). Probably even need to delete this option.
     */
    if (!use_tx || !producer_channel->usable())
        return;

    std::atomic<bool> answer_received = false, wait_rollback = false;

    producer_channel->commitTransaction()
    .onSuccess([&]()
    {
        answer_received = true;
        LOG_TRACE(log, "All messages were successfully published");
    })
    .onError([&](const char * message1)
    {
        answer_received = true;
        wait_rollback = true;
        LOG_TRACE(log, "Publishing not successful: {}", message1);

        producer_channel->rollbackTransaction()
        .onSuccess([&]()
        {
            wait_rollback = false;
        })
        .onError([&](const char * message2)
        {
            wait_rollback = false;
            LOG_ERROR(log, "Failed to rollback transaction: {}", message2);
        });
    });

    while (!answer_received || wait_rollback)
    {
        iterateEventLoop();
    }
}


void WriteBufferToRabbitMQProducer::nextImpl()
{
    chunks.push_back(std::string());
    chunks.back().resize(chunk_size);
    set(chunks.back().data(), chunk_size);
}


void WriteBufferToRabbitMQProducer::iterateEventLoop()
{
    event_handler->iterateLoop();
}

}
