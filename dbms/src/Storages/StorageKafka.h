#pragma once
#include <Common/config.h>
#if USE_RDKAFKA

#include <mutex>

#include <ext/shared_ptr_helper.h>
#include <Core/NamesAndTypes.h>
#include <Storages/IStorage.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Poco/Event.h>

struct rd_kafka_s;
struct rd_kafka_conf_s;

namespace DB
{

class StorageKafka;

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
    std::string getDatabaseName() const { return database_name; }

    const NamesAndTypesList & getColumnsListImpl() const override { return *columns; }

    void startup() override;
    void shutdown() override;

    BlockInputStreams read(
        const Names & column_names,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum & processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    void rename(const String & /*new_path_to_db*/, const String & new_database_name, const String & new_table_name) override
    {
        table_name = new_table_name;
        database_name = new_database_name;
    }

    void updateDependencies() override;

private:
    String table_name;
    String database_name;
    Context & context;
    NamesAndTypesListPtr columns;
    Names topics;
    const String format_name;
    const String schema_name;
    struct rd_kafka_conf_s * conf;
    struct rd_kafka_s * consumer;

    std::mutex mutex;
    Poco::Logger * log;
    Poco::Event cancel_event;
    std::atomic<bool> is_cancelled{false};
    std::thread stream_thread;

    void streamThread();
    void streamToViews();

protected:
    StorageKafka(
        const std::string & table_name_,
        const std::string & database_name_,
        Context & context_,
        NamesAndTypesListPtr columns_,
        const NamesAndTypesList & materialized_columns_,
        const NamesAndTypesList & alias_columns_,
        const ColumnDefaults & column_defaults_,
        const String & brokers_, const String & group_, const Names & topics_,
        const String & format_name_, const String & schema_name_);
};

}

#endif
