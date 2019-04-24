#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Core/NamesAndTypes.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Storages/IStorage.h>
#include <Storages/Kafka/ReadBufferFromKafkaConsumer.h>
#include <Poco/Event.h>
#include <Poco/Semaphore.h>
#include <ext/shared_ptr_helper.h>

#include <cppkafka/cppkafka.h>
#include <mutex>

namespace DB
{

/** Implements a Kafka queue table engine that can be used as a persistent queue / buffer,
  * or as a basic building block for creating pipelines with a continuous insertion / ETL.
  */
class StorageKafka : public ext::shared_ptr_helper<StorageKafka>, public IStorage
{
    friend class KafkaBlockInputStream;
    friend class KafkaBlockOutputStream;

public:
    std::string getName() const override { return "Kafka"; }
    std::string getTableName() const override { return table_name; }
    std::string getDatabaseName() const override { return database_name; }

    void startup() override;
    void shutdown() override;

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    void rename(const String & /* new_path_to_db */, const String & new_database_name, const String & new_table_name) override
    {
        table_name = new_table_name;
        database_name = new_database_name;
    }

    void updateDependencies() override;

private:
    // Configuration and state
    String table_name;
    String database_name;
    Context global_context;
    Names topics;
    const String brokers;
    const String group;
    const String format_name;
    // Optional row delimiter for generating char delimited stream
    // in order to make various input stream parsers happy.
    char row_delimiter;
    const String schema_name;
    /// Total number of consumers
    size_t num_consumers;
    /// Maximum block size for insertion into this table
    UInt64 max_block_size;
    /// Number of actually created consumers.
    /// Can differ from num_consumers in case of exception in startup() (or if startup() hasn't been called).
    /// In this case we still need to be able to shutdown() properly.
    size_t num_created_consumers = 0;
    Poco::Logger * log;

    // Consumer list
    Poco::Semaphore semaphore;
    std::mutex mutex;
    std::vector<BufferPtr> buffers; /// available buffers for Kafka consumers

    size_t skip_broken;

    // Stream thread
    BackgroundSchedulePool::TaskHolder task;
    std::atomic<bool> stream_cancelled{false};

    cppkafka::Configuration createConsumerConfiguration();
    BufferPtr createBuffer();
    BufferPtr claimBuffer();
    BufferPtr tryClaimBuffer(long wait_ms);
    void pushBuffer(BufferPtr buf);

    void streamThread();
    bool streamToViews();
    bool checkDependencies(const String & database_name, const String & table_name);

protected:
    StorageKafka(
        const std::string & table_name_,
        const std::string & database_name_,
        Context & context_,
        const ColumnsDescription & columns_,
        const String & brokers_, const String & group_, const Names & topics_,
        const String & format_name_, char row_delimiter_, const String & schema_name_,
        size_t num_consumers_, UInt64 max_block_size_, size_t skip_broken);
};

}
