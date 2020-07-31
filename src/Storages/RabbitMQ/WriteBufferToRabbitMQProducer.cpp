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
        std::pair<String, UInt16> & parsed_address_,
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
        , parsed_address(parsed_address_)
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

    /// New coonection for each publisher because cannot publish from different threads with the same connection.(https://github.com/CopernicaMarketingSoftware/AMQP-CPP/issues/128#issuecomment-300780086)
    setupConnection(0);
    setupChannel(0);

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


void WriteBufferToRabbitMQProducer::setupConnection(bool remove_prev_connection)
{
    if (remove_prev_connection && connection)
    {
        connection->close();
        connection.release();
    }

    connection = std::make_unique<AMQP::TcpConnection>(event_handler.get(), AMQP::Address(parsed_address.first, parsed_address.second, AMQP::Login(login_password.first, login_password.second), "/"));

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
}


void WriteBufferToRabbitMQProducer::setupChannel(bool remove_prev_channel)
{
    if (remove_prev_channel && producer_channel)
    {
        producer_channel->close();
        producer_channel.release();
    }

    producer_channel = std::make_unique<AMQP::TcpChannel>(connection.get());
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
        /// Same as here https://www.rabbitmq.com/blog/2011/02/10/introducing-publisher-confirms/
        remove_confirmed_tag = [&](uint64_t received_delivery_tag, bool multiple)
        {
            std::lock_guard lock(mutex);
            auto found_tag_pos = delivery_tags_record.find(received_delivery_tag);
            if (found_tag_pos != delivery_tags_record.end())
            {
                if (multiple)
                {
                    ++found_tag_pos;
                    delivery_tags_record.erase(delivery_tags_record.begin(), found_tag_pos);
                }
                else
                    delivery_tags_record.erase(found_tag_pos);
            }
        };

        /* if persistent == true, onAck is received when message is persisted to disk or when it is consumed on every queue. If fails, it
         * will be requed in returned_callback. If persistent == false, message is confirmed the moment it is enqueued. If fails, it is
         * not requeued. First option is two times slower than the second, so default is second and the first is turned on in table setting.
         */
        producer_channel->confirmSelect()
        .onAck([&](uint64_t acked_delivery_tag, bool multiple)
        {
            remove_confirmed_tag(acked_delivery_tag, multiple);
        })
        .onNack([&](uint64_t nacked_delivery_tag, bool multiple, bool /* requeue */)
        {
            if (!persistent)
                remove_confirmed_tag(nacked_delivery_tag, multiple);
        });
    }
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

        std::lock_guard lock(mutex);
        delivery_tags_record.insert(delivery_tags_record.end(), delivery_tag);
    }
}


void WriteBufferToRabbitMQProducer::writingFunc()
{
    String payload;
    UInt64 message_id = 0;

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

            ++message_id;
            if (wait_num)
                message_id %= wait_num;

            /// Delivery mode is 1 or 2. 1 is default. 2 makes a message durable, but makes performance 1.5-2 times worse.
            if (persistent)
                envelope.setDeliveryMode(2);

            if (exchange_type == AMQP::ExchangeType::consistent_hash)
            {
                producer_channel->publish(exchange_name, std::to_string(message_id), envelope).onReturned(returned_callback);
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

            if (message_id % BATCH == 0)
                iterateEventLoop();
        }

        if (wait_num.load() && delivery_tags_record.empty())
        {
            wait_all.store(false);
            LOG_DEBUG(log, "All messages are successfully published");
        }
        else
        {
            iterateEventLoop();
        }

        /// Most channel based errors result in channel closure, which is very likely to trigger connection closure.
        if (connection->usable() && connection->ready() && !producer_channel->usable())
        {
            LOG_DEBUG(log, "Channel is not usable. Creating a new one");
            setupChannel(1);
        }
        else if (!connection->usable() || !connection->ready())
        {
            LOG_DEBUG(log, "Connection is not usable. Creating a new one");
            setupConnection(1);
            setupChannel(1);
        }
    }

    LOG_DEBUG(log, "Delivered messages");
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
