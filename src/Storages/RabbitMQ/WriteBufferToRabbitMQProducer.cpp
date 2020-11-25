#include <Storages/RabbitMQ/WriteBufferToRabbitMQProducer.h>

#include <Core/Block.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/Context.h>
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
static const auto BATCH = 1000;
static const auto RETURNED_LIMIT = 50000;

namespace ErrorCodes
{
    extern const int CANNOT_CONNECT_RABBITMQ;
}

WriteBufferToRabbitMQProducer::WriteBufferToRabbitMQProducer(
        std::pair<String, UInt16> & parsed_address_,
        const Context & global_context,
        const std::pair<String, String> & login_password_,
        const Names & routing_keys_,
        const String & exchange_name_,
        const AMQP::ExchangeType exchange_type_,
        const size_t channel_id_base_,
        const bool persistent_,
        std::atomic<bool> & wait_confirm_,
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
        , channel_id_base(std::to_string(channel_id_base_))
        , persistent(persistent_)
        , wait_confirm(wait_confirm_)
        , payloads(BATCH)
        , returned(RETURNED_LIMIT)
        , log(log_)
        , delim(delimiter)
        , max_rows(rows_per_message)
        , chunk_size(chunk_size_)
{

    loop = std::make_unique<uv_loop_t>();
    uv_loop_init(loop.get());
    event_handler = std::make_unique<RabbitMQHandler>(loop.get(), log);

    if (setupConnection(false))
    {
        setupChannel();
    }
    else
    {
        if (!connection->closed())
             connection->close(true);

        throw Exception("Cannot connect to RabbitMQ host: " + parsed_address.first + ", port: " + std::to_string(parsed_address.second),
                ErrorCodes::CANNOT_CONNECT_RABBITMQ);
    }

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

    size_t cnt_retries = 0;
    while (!connection->closed() && ++cnt_retries != RETRIES_MAX)
    {
        event_handler->iterateLoop();
        std::this_thread::sleep_for(std::chrono::milliseconds(CONNECT_SLEEP));
    }

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

        for (auto i = chunks.begin(), end = --chunks.end(); i != end; ++i)
            payload.append(*i);

        payload.append(last_chunk, 0, last_chunk_size);

        rows = 0;
        chunks.clear();
        set(nullptr, 0);

        ++payload_counter;
        payloads.push(std::make_pair(payload_counter, payload));
    }
}


bool WriteBufferToRabbitMQProducer::setupConnection(bool reconnecting)
{
    size_t cnt_retries = 0;

    if (reconnecting)
    {
        connection->close();

        while (!connection->closed() && ++cnt_retries != RETRIES_MAX)
            event_handler->iterateLoop();

        if (!connection->closed())
            connection->close(true);

        LOG_TRACE(log, "Trying to set up connection");
    }

    connection = std::make_unique<AMQP::TcpConnection>(event_handler.get(),
            AMQP::Address(parsed_address.first, parsed_address.second, AMQP::Login(login_password.first, login_password.second), "/"));

    cnt_retries = 0;
    while (!connection->ready() && ++cnt_retries != RETRIES_MAX)
    {
        event_handler->iterateLoop();
        std::this_thread::sleep_for(std::chrono::milliseconds(CONNECT_SLEEP));
    }

    return event_handler->connectionRunning();
}


void WriteBufferToRabbitMQProducer::setupChannel()
{
    producer_channel = std::make_unique<AMQP::TcpChannel>(connection.get());

    producer_channel->onError([&](const char * message)
    {
        LOG_ERROR(log, "Producer's channel {} error: {}", channel_id, message);

        /// Channel is not usable anymore. (https://github.com/CopernicaMarketingSoftware/AMQP-CPP/issues/36#issuecomment-125112236)
        producer_channel->close();

        /* Save records that have not received ack/nack from server before channel closure. They are removed and pushed back again once
         * they are republished because after channel recovery they will acquire new delivery tags, so all previous records become invalid
         */
        for (const auto & record : delivery_record)
            returned.tryPush(record.second);

        LOG_DEBUG(log, "Producer on channel {} hasn't confirmed {} messages, {} waiting to be published",
                channel_id, delivery_record.size(), payloads.size());

        /// Delivery tags are scoped per channel.
        delivery_record.clear();
        delivery_tag = 0;
    });

    producer_channel->onReady([&]()
    {
        channel_id = channel_id_base + std::to_string(channel_id_counter++);
        LOG_DEBUG(log, "Producer's channel {} is ready", channel_id);

        /* if persistent == true, onAck is received when message is persisted to disk or when it is consumed on every queue. If fails,
         * onNack() is received. If persistent == false, message is confirmed the moment it is enqueued. First option is two times
         * slower than the second, so default is second and the first is turned on in table setting.
         *
         * "Publisher confirms" are implemented similar to strategy#3 here https://www.rabbitmq.com/tutorials/tutorial-seven-java.html
         */
        producer_channel->confirmSelect()
        .onAck([&](uint64_t acked_delivery_tag, bool multiple)
        {
            removeRecord(acked_delivery_tag, multiple, false);
        })
        .onNack([&](uint64_t nacked_delivery_tag, bool multiple, bool /* requeue */)
        {
            removeRecord(nacked_delivery_tag, multiple, true);
        });
    });
}


void WriteBufferToRabbitMQProducer::removeRecord(UInt64 received_delivery_tag, bool multiple, bool republish)
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
        }
        else
        {
            if (republish)
                returned.tryPush(record_iter->second);

            delivery_record.erase(record_iter);
        }
    }
    /// else is theoretically not possible
}


void WriteBufferToRabbitMQProducer::publish(ConcurrentBoundedQueue<std::pair<UInt64, String>> & messages, bool republishing)
{
    std::pair<UInt64, String> payload;

    /* It is important to make sure that delivery_record.size() is never bigger than returned.size(), i.e. number if unacknowledged
     * messages cannot exceed returned.size(), because they all might end up there
     */
    while (!messages.empty() && producer_channel->usable() && delivery_record.size() < RETURNED_LIMIT)
    {
        messages.pop(payload);
        AMQP::Envelope envelope(payload.second.data(), payload.second.size());

        /// if headers exchange is used, routing keys are added here via headers, if not - it is just empty
        AMQP::Table message_settings = key_arguments;

        /* There is the case when connection is lost in the period after some messages were published and before ack/nack was sent by the
         * server, then it means that publisher will never know whether those messages were delivered or not, and therefore those records
         * that received no ack/nack before connection loss will be republished (see onError() callback), so there might be duplicates. To
         * let consumer know that received message might be a possible duplicate - a "republished" field is added to message metadata
         */
        message_settings["republished"] = std::to_string(republishing);
        envelope.setHeaders(message_settings);

        /* Adding here a messageID property to message metadata. Since RabbitMQ does not guarantee exactly-once delivery, then on the
         * consumer side "republished" field of message metadata can be checked and, if it set to 1, consumer might also check "messageID"
         * property. This way detection of duplicates is guaranteed
         */
        envelope.setMessageID(std::to_string(payload.first));

        /// Delivery mode is 1 or 2. 1 is default. 2 makes a message durable, but makes performance 1.5-2 times worse
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

        /// This is needed for "publisher confirms", which guarantees at-least-once delivery
        ++delivery_tag;
        delivery_record.insert(delivery_record.end(), {delivery_tag, payload});

        /// Need to break at some point to let event loop run, because no publishing actually happens before looping
        if (delivery_tag % BATCH == 0)
            break;
    }

    iterateEventLoop();
}


void WriteBufferToRabbitMQProducer::writingFunc()
{
    while ((!payloads.empty() || wait_all) && wait_confirm.load())
    {
        /* Publish main paylods only when there are no returned messages. This way it is ensured that returned messages are republished
         * as fast as possible and no new publishes are made before returned messages are handled
         */
        if (!returned.empty() && producer_channel->usable())
            publish(returned, true);
        else if (!payloads.empty() && producer_channel->usable())
            publish(payloads, false);

        iterateEventLoop();

        if (wait_num.load() && delivery_record.empty() && payloads.empty() && returned.empty())
            wait_all = false;
        else if ((!producer_channel->usable() && event_handler->connectionRunning()) || (!event_handler->connectionRunning() && setupConnection(true)))
            setupChannel();
    }

    LOG_DEBUG(log, "Prodcuer on channel {} completed", channel_id);
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
