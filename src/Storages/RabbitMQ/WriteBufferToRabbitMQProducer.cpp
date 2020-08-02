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

namespace ErrorCodes
{
    extern const int CANNOT_CONNECT_RABBITMQ;
}

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
        Poco::Logger * log_,
        const bool use_transactional_channel_,
        const bool persistent_,
        std::optional<char> delimiter,
        size_t rows_per_message,
        size_t chunk_size_)
        : WriteBuffer(nullptr, 0)
        , parsed_address(parsed_address_)
        , login_password(login_password_)
        , routing_keys(routing_keys_)
        , exchange_name(exchange_name_)
        , exchange_type(exchange_type_)
        , use_transactional_channel(use_transactional_channel_)
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

    /// New coonection for each publisher because cannot publish from different threads with the same connection.(https://github.com/CopernicaMarketingSoftware/AMQP-CPP/issues/128#issuecomment-300780086)
    if (setupConnection())
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

        payloads.push(payload);
        ++payload_counter;
    }
}


bool WriteBufferToRabbitMQProducer::setupConnection()
{
    connection = std::make_unique<AMQP::TcpConnection>(event_handler.get(), AMQP::Address(parsed_address.first, parsed_address.second, AMQP::Login(login_password.first, login_password.second), "/"));

    LOG_TRACE(log, "Trying to set up connection");
    size_t cnt_retries = 0;

    while (!connection->ready() && ++cnt_retries != RETRIES_MAX)
    {
        event_handler->iterateLoop();
        std::this_thread::sleep_for(std::chrono::milliseconds(CONNECT_SLEEP));
    }

    if (!connection->ready())
        return false;

    return true;
}


void WriteBufferToRabbitMQProducer::setupChannel()
{
    producer_channel = std::make_unique<AMQP::TcpChannel>(connection.get());
    producer_channel->onError([&](const char * message)
    {
        LOG_DEBUG(log, "Producer error: {}. Currently {} messages have not been confirmed yet, {} messages are waiting to be published",
                message, delivery_tags_record.size(), payloads.size());

        /// Means channel ends up in an error state and is not usable anymore.
        producer_channel->close();
    });

    producer_channel->onReady([&]()
    {
        LOG_DEBUG(log, "Producer channel is ready");

        /// Delivery tags are scoped per channel.
        delivery_tags_record.clear();
        delivery_tag = 0;

        if (use_transactional_channel)
        {
            producer_channel->startTransaction();
        }
        else
        {
            /* if persistent == true, onAck is received when message is persisted to disk or when it is consumed on every queue. If fails,
             * it will be requed in returned_callback. If persistent == false, message is confirmed the moment it is enqueued. If fails, it
             * is not requeued. First option is two times slower than the second, so default is second and the first is turned on in table
             * setting. Persistent message is not requeued if it is unroutable, i.e. no queues are bound to given exchange with the given
             * routing key - this is a responsibility of a client. It can be requeued in this case if AMQP::mandatory is set, but pointless.
             */
            producer_channel->confirmSelect()
            .onAck([&](uint64_t acked_delivery_tag, bool multiple)
            {
                removeConfirmed(acked_delivery_tag, multiple);
            })
            .onNack([&](uint64_t nacked_delivery_tag, bool multiple, bool /* requeue */)
            {
                if (!persistent)
                    removeConfirmed(nacked_delivery_tag, multiple);
            });
        }
    });
}


void WriteBufferToRabbitMQProducer::removeConfirmed(UInt64 received_delivery_tag, bool multiple)
{
    /// Same as here https://www.rabbitmq.com/blog/2011/02/10/introducing-publisher-confirms/
    std::lock_guard lock(mutex);
    auto found_tag_pos = delivery_tags_record.find(received_delivery_tag);
    if (found_tag_pos != delivery_tags_record.end())
    {
        /// If multiple is true, then all delivery tags up to and including current are confirmed.
        if (multiple)
        {
            ++found_tag_pos;
            delivery_tags_record.erase(delivery_tags_record.begin(), found_tag_pos);
            //LOG_DEBUG(log, "Confirmed all delivery tags up to {}", received_delivery_tag);
        }
        else
        {
            delivery_tags_record.erase(found_tag_pos);
            //LOG_DEBUG(log, "Confirmed delivery tag {}", received_delivery_tag);
        }
    }
}


void WriteBufferToRabbitMQProducer::publish(ConcurrentBoundedQueue<String> & messages)
{
    String payload;
    while (!messages.empty())
    {
        messages.pop(payload);
        AMQP::Envelope envelope(payload.data(), payload.size());

        /// Delivery mode is 1 or 2. 1 is default. 2 makes a message durable, but makes performance 1.5-2 times worse.
        if (persistent)
            envelope.setDeliveryMode(2);

        if (exchange_type == AMQP::ExchangeType::consistent_hash)
        {
            producer_channel->publish(exchange_name, std::to_string(delivery_tag), envelope).onReturned(returned_callback);
        }
        else if (exchange_type == AMQP::ExchangeType::headers)
        {
            envelope.setHeaders(key_arguments);
            producer_channel->publish(exchange_name, "", envelope).onReturned(returned_callback);
        }
        else
        {
            producer_channel->publish(exchange_name, routing_keys[0], envelope).onReturned(returned_callback);
        }

        if (producer_channel->usable())
        {
            ++delivery_tag;
            delivery_tags_record.insert(delivery_tags_record.end(), delivery_tag);

            if (delivery_tag % BATCH == 0)
                break;
        }
        else
        {
            break;
        }
    }

    iterateEventLoop();
}

/* Currently implemented “asynchronous publisher confirms” - does not stop after each publish to wait for each individual confirm. An
 * asynchronous publisher may have any number of messages in-flight (unconfirmed) at a time.
 * Synchronous publishing is where after each publish need to wait for the acknowledgement (ack/nack - see confirmSelect() in channel
 * declaration), which is very slow because takes starting event loop and waiting for corresponding callback - can really take a while.
 *
 * Async publishing works well in all failure cases except for connection failure, because if connection fails - not all Ack/Nack might be
 * receieved from the server (and even if all messages were successfully delivered, publisher will not be able to know it). Also in this
 * case onReturned callback will not be received, so loss is possible for messages that were published but have not received confirm from
 * server before connection loss, because then publisher won't know if message was delivered or not.
 *
 * To make it a delivery with no loss and minimal possible amount of duplicates - need to use synchronous publishing (which is too slow).
 * With async publishing at-least-once delivery is achieved with (batch) publishing and manual republishing in case when not all delivery
 * tags were confirmed (ack/nack) before connection loss. Here the maximum number of possible duplicates is no more than batch size.
 * (Manual last batch republishing is only for case of connection loss, in all other failure cases - onReturned callback will be received.)
 *
 * So currently implemented async batch publishing, but for now without manual republishing (because still in doubt how to do it nicely,
 * but current idea is to store in delivery_tags_record not just delivery tags, but pair: (delivery_tag, message). As currently once the
 * publisher receives acknowledgement from the server that the message was sucessfully delivered - a "confirmListener" will delete its
 * delivery tag from the set of pending acknowledgemens, then we can as well delete the payload. If connection fails, undeleted delivery
 * tags indicate messages, whose fate is unknown, so corresponding payloads should be republished.)
*/
void WriteBufferToRabbitMQProducer::writingFunc()
{
    returned_callback = [&](const AMQP::Message & message, int16_t code, const std::string & description)
    {
        returned.tryPush(std::string(message.body(), message.size()));
        LOG_DEBUG(log, "Message returned with code: {}, description: {}. Republishing", code, description);

        /* Here can be added a value to AMQP::Table field of AMQP::Envelope (and then it should be queue<AMQP::Envelope> instead of
         * queue<String>) - to indicate that message was republished. Later a consumer will be able to extract this field and understand
         * that this message was republished and can probably be a duplicate (as RabbitMQ does not guarantee exactly-once delivery).
         */
    };

    while (!payloads.empty() || wait_all)
    {
        if (!returned.empty() && producer_channel->usable())
            publish(returned);
        else if (!payloads.empty() && delivery_tags_record.empty() && producer_channel->usable())
            publish(payloads);

        iterateEventLoop();

        if (wait_num.load() && delivery_tags_record.empty() && payloads.empty())
            wait_all.store(false);
        else if ((!producer_channel->usable() && connection->usable()) || (!connection->usable() && setupConnection()))
            setupChannel();
    }

    LOG_DEBUG(log, "Processing ended");
}


void WriteBufferToRabbitMQProducer::commit()
{
    if (!use_transactional_channel)
        return;

    std::atomic<bool> answer_received = false, wait_rollback = false;
    producer_channel->commitTransaction()
    .onSuccess([&]()
    {
        answer_received = true;
        wait_all.store(false);
        LOG_TRACE(log, "All messages were successfully published");
    })
    .onError([&](const char * message1)
    {
        answer_received = true;
        wait_all.store(false);
        LOG_TRACE(log, "Publishing not successful: {}", message1);

        wait_rollback = true;
        producer_channel->rollbackTransaction()
        .onSuccess([&]()
        {
            wait_rollback = false;
        })
        .onError([&](const char * message2)
        {
            LOG_ERROR(log, "Failed to rollback transaction: {}", message2);
            wait_rollback = false;
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
