#pragma once
#include <Core/BackgroundSchedulePool.h>
#include <Storages/IStorage.h>
#include <Interpreters/Context.h>
#include <Poco/Semaphore.h>
#include <ext/shared_ptr_helper.h>
#include <mutex>
#include <atomic>
#include <event2/event.h>

namespace DB
{

using ChannelPtr = std::shared_ptr<AMQP::TcpChannel>;

class StorageRabbitMQ final: public ext::shared_ptr_helper<StorageRabbitMQ>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageRabbitMQ>;

public:
    std::string getName() const override { return "RabbitMQ"; }
    bool supportsSettings() const override { return true; }

protected:
    StorageRabbitMQ(
            const StorageID & table_id_,
            Context & context_,
            const ColumnsDescription & columns_,
            const String & host_port_,
            const String & routing_key_, const String & exchange_name_, 
            const String & format_name_, char row_delimiter_,
            size_t num_consumers_, bool bind_by_id_, size_t num_queues_, bool hash_exchange);

private:
    Context global_context;
    Context rabbitmq_context;

    String routing_key;
    const String exchange_name;

    const String format_name;
    char row_delimiter;
    size_t num_consumers;
    size_t num_created_consumers = 0;

    bool bind_by_id;
    size_t num_queues;
    const bool hash_exchange;

    Poco::Logger * log;
    std::pair<std::string, UInt16> parsed_address;

    Poco::Semaphore semaphore;
    std::mutex mutex;

    size_t consumer_id = 0;

    BackgroundSchedulePool::TaskHolder task;
    std::atomic<bool> stream_cancelled{false};
};

}
