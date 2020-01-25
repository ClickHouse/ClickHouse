#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Storages/IStorage.h>
#include <Storages/RabbitMQ/Buffer_fwd.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>

#include <Poco/Semaphore.h>
#include <ext/shared_ptr_helper.h>

#include <mutex>
#include <atomic>


namespace DB
{

class StorageRabbitMQ : public ext::shared_ptr_helper<StorageRabbitMQ>, public IStorage
{
    friend struct ext::shared_ptr_helper<StorageRabbitMQ>;
public:
    std::string getName() const override { return "RabbitMQ"; }
    std::string getTableName() const override { return table_name; }
    std::string getDatabaseName() const override { return database_name; }

    bool supportsSettings() const override { return true; }

    void startup() override;
    void shutdown() override;

    BlockInputStreams read(
            const Names & column_names,
            const SelectQueryInfo & query_info,
            const Context & context,
            QueryProcessingStage::Enum processed_stage,
            size_t max_block_size,
            unsigned num_streams) override;

    BlockOutputStreamPtr write(
            const ASTPtr & query,
            const Context & context) override;

    void rename(const String & /* new_path_to_db */,
            const String & new_database_name,
            const String & new_table_name,
            TableStructureWriteLockHolder &) override;

    void pushReadBuffer(ConsumerBufferPtr buf);
    ConsumerBufferPtr popReadBuffer();
    ConsumerBufferPtr popReadBuffer(std::chrono::milliseconds timeout);

    ProducerBufferPtr createWriteBuffer();

    const Names & getRoutingKeys() const { return routing_keys; }
    const String & getFormatName() const { return format_name; }
    const auto & skipBroken() const { return skip_broken; }

protected:
    StorageRabbitMQ(
            const std::string & table_name_,
            const std::string & database_name_,
            Context & context_,
            const ColumnsDescription & columns_,
            const String & brokers_, const Names & routing_keys_,
            const String & format_name_, char row_delimiter_,
            size_t num_consumers_, UInt64 max_block_size_, size_t skip_broken);

private:
    String table_name;
    String database_name;
    Context global_context;
    const String brokers;
    Names routing_keys;
    const String format_name;
    char row_delimiter;
    size_t num_consumers; /// total number of consumers
    UInt64 max_block_size; /// maximum block size for insertion into this table

    size_t num_created_consumers = 0; /// number of actually created consumers.

    Poco::Logger * log;
    Poco::Semaphore semaphore;

    std::vector<ConsumerBufferPtr> buffers; /// available buffers for RabbitMQ consumers

    size_t skip_broken;

    ConsumerBufferPtr createReadBuffer();

    RabbitMQHandler connection_handler;
    AMQP::Connection connection;

};

}