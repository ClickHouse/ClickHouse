#include <Storages/RabbitMQ/WriteBufferToRabbitMQProducer.h>
#include "Core/Block.h"
#include "Columns/ColumnString.h"
#include "Columns/ColumnsNumber.h"
#include <common/logger_useful.h>
#include <amqpcpp.h>
#include <chrono>
#include <thread>
#include <atomic>


namespace DB
{

enum
{
    Connection_setup_sleep = 200,
    Connection_setup_retries_max = 1000,
    Buffer_limit_to_flush = 5000 /// It is important to keep it low in order not to kill consumers
};

WriteBufferToRabbitMQProducer::WriteBufferToRabbitMQProducer(
        std::pair<std::string, UInt16> & parsed_address,
        const String & routing_key_,
        const String & exchange_,
        Poco::Logger * log_,
        const size_t num_queues_,
        const bool bind_by_id_,
        const bool hash_exchange_,
        std::optional<char> delimiter,
        size_t rows_per_message,
        size_t chunk_size_)
        : WriteBuffer(nullptr, 0)
        , routing_key(routing_key_)
        , exchange_name(exchange_)
        , log(log_)
        , num_queues(num_queues_)
        , bind_by_id(bind_by_id_)
        , hash_exchange(hash_exchange_)
        , delim(delimiter)
        , max_rows(rows_per_message)
        , chunk_size(chunk_size_)
        , producerEvbase(event_base_new())
        , eventHandler(producerEvbase, log)
        , connection(&eventHandler, AMQP::Address(parsed_address.first, parsed_address.second, AMQP::Login("root", "clickhouse"), "/"))
{
    /* The reason behind making a separate connection for each concurrent producer is explained here:
     * https://github.com/CopernicaMarketingSoftware/AMQP-CPP/issues/128#issuecomment-300780086 - publishing from
     * different threads (as outputStreams are asynchronous) with the same connection leads to internal libary errors.
     */
    size_t cnt_retries = 0;
    while (!connection.ready() && ++cnt_retries != Connection_setup_retries_max)
    {
        event_base_loop(producerEvbase, EVLOOP_NONBLOCK | EVLOOP_ONCE);
        std::this_thread::sleep_for(std::chrono::milliseconds(Connection_setup_sleep));
    }

    if (!connection.ready())
    {
        LOG_ERROR(log, "Cannot set up connection for producer!");
    }

    producer_channel = std::make_shared<AMQP::TcpChannel>(&connection);
}


WriteBufferToRabbitMQProducer::~WriteBufferToRabbitMQProducer()
{
    flush();
    connection.close();

    assert(rows == 0 && chunks.empty());
}


void WriteBufferToRabbitMQProducer::count_row()
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

        messages.emplace_back(payload);
        ++message_counter;

        if (messages.size() >= Buffer_limit_to_flush)
        {
            flush();
        }
    }
}


void WriteBufferToRabbitMQProducer::flush()
{
    /* The reason for accumulating payloads and not publishing each of them at once in count_row() is that publishing
     * needs to be wrapped inside declareExchange() callback and it is too expensive in terms of time to declare it
     * each time we publish. Declaring it once and then publishing without wrapping inside onSuccess callback leads to
     * exchange becoming inactive at some point and part of messages is lost as a result.
     */
    std::atomic<bool> exchange_declared = false, exchange_error = false;

    producer_channel->declareExchange(exchange_name + "_direct", AMQP::direct, AMQP::passive)
    .onSuccess([&]()
    {
        for (auto & payload : messages)
        {
            if (!message_counter)
                return;

            next_queue = next_queue % num_queues + 1;

            if (bind_by_id || hash_exchange)
            {
                producer_channel->publish(exchange_name, std::to_string(next_queue), payload);
            }
            else
            {
                producer_channel->publish(exchange_name, routing_key, payload);
            }

            --message_counter;
        }

        exchange_declared = true;
        messages.clear();
    })
    .onError([&](const char * message)
    {
        exchange_error = true;
        exchange_declared = false;
        LOG_ERROR(log, "Exchange was not declared: {}", message);
    });

    while (!exchange_declared && !exchange_error)
    {
        startEventLoop(exchange_declared);
    }
}


void WriteBufferToRabbitMQProducer::nextImpl()
{
    chunks.push_back(std::string());
    chunks.back().resize(chunk_size);
    set(chunks.back().data(), chunk_size);
}


void WriteBufferToRabbitMQProducer::startEventLoop(std::atomic<bool> & check_param)
{
    eventHandler.start(check_param);
}

}
