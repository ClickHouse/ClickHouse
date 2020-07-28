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

static const auto QUEUE_SIZE = 50000;
static const auto CONNECT_SLEEP = 200;
static const auto RETRIES_MAX = 1000;
static const auto LOOP_WAIT = 10;
static const auto BATCH = 10000;

WriteBufferToRabbitMQProducer::WriteBufferToRabbitMQProducer(
        std::pair<String, UInt16> & parsed_address,
        Context & global_context,
        const std::pair<String, String> & login_password_,
        const Names & routing_keys_,
        const String & exchange_name_,
        const AMQP::ExchangeType exchange_type_,
        Poco::Logger * log_,
        size_t num_queues_,
        const bool use_transactional_channel_,
        const bool persistent_,
        std::optional<char> delimiter,
        size_t rows_per_message,
        size_t chunk_size_)
        : WriteBuffer(nullptr, 0)
        , login_password(login_password_)
        , routing_keys(routing_keys_)
        , exchange_name(exchange_name_)
        , exchange_type(exchange_type_)
        , num_queues(num_queues_)
        , use_transactional_channel(use_transactional_channel_)
        , persistent(persistent_)
        , payloads(QUEUE_SIZE * num_queues)
        , log(log_)
        , delim(delimiter)
        , max_rows(rows_per_message)
        , chunk_size(chunk_size_)
{

    loop = std::make_unique<uv_loop_t>();
    uv_loop_init(loop.get());

    event_handler = std::make_unique<RabbitMQHandler>(loop.get(), log);
    connection = std::make_unique<AMQP::TcpConnection>(event_handler.get(), AMQP::Address(parsed_address.first, parsed_address.second, AMQP::Login(login_password.first, login_password.second), "/"));

    /// New coonection for each publisher because cannot publish from different threads.(https://github.com/CopernicaMarketingSoftware/AMQP-CPP/issues/128#issuecomment-300780086)
    size_t cnt_retries = 0;
    while (!connection->ready() && ++cnt_retries != RETRIES_MAX)
    {
        event_handler->iterateLoop();
        std::this_thread::sleep_for(std::chrono::milliseconds(CONNECT_SLEEP));
    }

    if (!connection->ready())
    {
        throw Exception("Cannot set up connection for producer", ErrorCodes::CANNOT_CONNECT_RABBITMQ);
    }

    producer_channel = std::make_shared<AMQP::TcpChannel>(connection.get());
    producer_channel->onError([&](const char * message)
    {
        LOG_ERROR(log, "Prodcuer error: {}", message);
    });

    if (use_transactional_channel)
    {
        producer_channel->startTransaction();
    }
    else
    {
        producer_channel->confirmSelect()
        .onAck([&](uint64_t deliveryTag, bool /* multiple */)
        {
            if (deliveryTag > last_processed)
                last_processed = deliveryTag;
        })
        .onNack([&](uint64_t /* deliveryTag */, bool /* multiple */, bool /* requeue */)
        {
        });
    }

    writing_task = global_context.getSchedulePool().createTask("RabbitMQWritingTask", [this]{ writingFunc(); });
    writing_task->deactivate();

    if (exchange_type == AMQP::ExchangeType::headers)
    {
        std::vector<String> matching;
        for (const auto & header : routing_keys)
        {
            boost::split(matching, header, [](char c){ return c == '='; });
            key_arguments[matching[0]] = matching[1];
            matching.clear();
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

        ++delivery_tag;
        payloads.push(payload);
    }
}


void WriteBufferToRabbitMQProducer::writingFunc()
{
    String payload;
    current = 0;

    auto returned_callback = [&](const AMQP::Message & message, int16_t /* code */, const std::string & /* description */)
    {
        payloads.push(std::string(message.body(), message.size()));
        //LOG_DEBUG(log, "Message returned with code: {}, description: {}. Republishing", code, description);
    };

    while ((!payloads.empty() || wait_all) && connection->usable())
    {
        while (!payloads.empty() && producer_channel->usable())
        {
            payloads.pop(payload);
            AMQP::Envelope envelope(payload.data(), payload.size());
            current = wait_num ? ++current % wait_num : ++current;

            /// Delivery mode is 1 or 2. 1 is default. 2 makes a message durable, but makes performance 1.5-2 times worse.
            if (persistent)
                envelope.setDeliveryMode(2);

            if (exchange_type == AMQP::ExchangeType::consistent_hash)
            {
                producer_channel->publish(exchange_name, std::to_string(current), envelope).onReturned(returned_callback);
            }
            else if (exchange_type == AMQP::ExchangeType::headers)
            {
                envelope.setHeaders(key_arguments);
                producer_channel->publish(exchange_name, "", envelope, key_arguments).onReturned(returned_callback);
            }
            else
            {
                producer_channel->publish(exchange_name, routing_keys[0], envelope).onReturned(returned_callback);
            }

            if (current % BATCH == 0)
                iterateEventLoop();
        }

        if (wait_num.load() && last_processed.load() >= wait_num.load())
        {
            wait_all.store(false);
            LOG_DEBUG(log, "All messages are successfully published");
        }
        else
        {
            iterateEventLoop();
        }
    }
}


void WriteBufferToRabbitMQProducer::finilizeProducer()
{
    if (use_transactional_channel)
    {
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
            wait_rollback = true;
            LOG_TRACE(log, "Publishing not successful: {}", message1);
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

        size_t count_retries = 0;
        while ((!answer_received || wait_rollback) && ++count_retries != RETRIES_MAX)
        {
            iterateEventLoop();
            std::this_thread::sleep_for(std::chrono::milliseconds(LOOP_WAIT));
        }
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
