#include <Storages/System/StorageSystemKafkaConsumers.h>

#if USE_RDKAFKA

#include <Access/ContextAccess.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/Kafka/StorageKafka.h>
#include <Storages/Kafka/StorageKafka2.h>
#include <base/Decimal_fwd.h>
#include <base/types.h>

namespace DB
{

ColumnsDescription StorageSystemKafkaConsumers::getColumnsDescription()
{
    // clang-format off
    return ColumnsDescription{
        {"database", std::make_shared<DataTypeString>(), "Database of the table with Kafka Engine."},
        {"table", std::make_shared<DataTypeString>(), "Name of the table with Kafka Engine."},
        {"consumer_id", std::make_shared<DataTypeString>(), "Kafka consumer identifier. Note, that a table can have many consumers. Specified by `kafka_num_consumers` parameter."},
        {"assignments.topic", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Kafka topic."},
        {"assignments.partition_id", std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>()), "Kafka partition id. Note, that only one consumer can be assigned to a partition."},
        {"assignments.current_offset", std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt64>()), "Current offset."},
        {"assignments.intent_size", std::make_shared<DataTypeArray>(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())), "The number of pushed, but not yet committed messages in new StorageKafka."},
        {"exceptions.time", std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>()), "Timestamp when the 10 most recent exceptions were generated."},
        {"exceptions.text", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Text of 10 most recent exceptions."},
        {"last_poll_time", std::make_shared<DataTypeDateTime>(), "Timestamp of the most recent poll."},
        {"num_messages_read", std::make_shared<DataTypeUInt64>(), "Number of messages read by the consumer."},
        {"last_commit_time", std::make_shared<DataTypeDateTime>(), "Timestamp of the most recent poll."},
        {"num_commits", std::make_shared<DataTypeUInt64>(), "Total number of commits for the consumer."},
        {"last_rebalance_time", std::make_shared<DataTypeDateTime>(), "Timestamp of the most recent Kafka rebalance."},
        {"num_rebalance_revocations", std::make_shared<DataTypeUInt64>(), "Number of times the consumer was revoked its partitions."},
        {"num_rebalance_assignments", std::make_shared<DataTypeUInt64>(), "Number of times the consumer was assigned to Kafka cluster."},
        {"is_currently_used", std::make_shared<DataTypeUInt8>(), "The flag which shows whether the consumer is in use."},
        {"last_used", std::make_shared<DataTypeDateTime64>(6), "The last time this consumer was in use."},
        {"rdkafka_stat", std::make_shared<DataTypeString>(), "Library internal statistic. Set statistics_interval_ms to 0 disable, default is 3000 (once in three seconds)."},
    };
    // clang-format on
}

void StorageSystemKafkaConsumers::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto tables_mark_dropped = DatabaseCatalog::instance().getTablesMarkedDropped();

    size_t index = 0;

    auto & database = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & table = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & consumer_id = assert_cast<ColumnString &>(*res_columns[index++]); //(number? or string? - single clickhouse table can have many consumers)

    auto & assignments_topics = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[index]).getData());
    auto & assignments_topics_offsets = assert_cast<ColumnArray &>(*res_columns[index++]).getOffsets();

    auto & assignments_partition_id = assert_cast<ColumnInt32 &>(assert_cast<ColumnArray &>(*res_columns[index]).getData());
    auto & assignments_partition_id_offsets = assert_cast<ColumnArray &>(*res_columns[index++]).getOffsets();

    auto & assignments_current_offset = assert_cast<ColumnInt64 &>(assert_cast<ColumnArray &>(*res_columns[index]).getData());
    auto & assignments_current_offset_offsets = assert_cast<ColumnArray &>(*res_columns[index++]).getOffsets();

    auto & assignments_intent_size = assert_cast<ColumnNullable &>(assert_cast<ColumnArray &>(*res_columns[index]).getData());
    auto & assignments_intent_size_offsets = assert_cast<ColumnArray &>(*res_columns[index++]).getOffsets();

    auto & exceptions_time = assert_cast<ColumnDateTime &>(assert_cast<ColumnArray &>(*res_columns[index]).getData());
    auto & exceptions_time_offset = assert_cast<ColumnArray &>(*res_columns[index++]).getOffsets();
    auto & exceptions_text = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[index]).getData());
    auto & exceptions_text_offset = assert_cast<ColumnArray &>(*res_columns[index++]).getOffsets();
    auto & last_poll_time = assert_cast<ColumnDateTime &>(*res_columns[index++]);
    auto & num_messages_read = assert_cast<ColumnUInt64 &>(*res_columns[index++]);
    auto & last_commit_time = assert_cast<ColumnDateTime &>(*res_columns[index++]);
    auto & num_commits = assert_cast<ColumnUInt64 &>(*res_columns[index++]);
    auto & last_rebalance_time = assert_cast<ColumnDateTime &>(*res_columns[index++]);
    auto & num_rebalance_revocations = assert_cast<ColumnUInt64 &>(*res_columns[index++]);
    auto & num_rebalance_assignments = assert_cast<ColumnUInt64 &>(*res_columns[index++]);
    auto & is_currently_used = assert_cast<ColumnUInt8 &>(*res_columns[index++]);
    auto & last_used = assert_cast<ColumnDateTime64 &>(*res_columns[index++]);
    auto & rdkafka_stat = assert_cast<ColumnString &>(*res_columns[index++]);

    const auto access = context->getAccess();
    size_t last_assignment_num = 0;
    size_t exceptions_num = 0;

    auto add_rows = [&](const DatabaseTablesIteratorPtr & it, auto & storage_kafka)
    {
        std::string database_str = it->databaseName();
        std::string table_str = it->name();

        auto safe_consumers = storage_kafka.getSafeConsumers();

        for (const auto & consumer : safe_consumers.consumers)
        {
            auto consumer_stat = consumer->getStat();

            database.insertData(database_str.data(), database_str.size());
            table.insertData(table_str.data(), table_str.size());

            consumer_id.insertData(consumer_stat.consumer_id.data(), consumer_stat.consumer_id.size());

            const auto num_assignments = consumer_stat.assignments.size();

            for (size_t num = 0; num < num_assignments; ++num)
            {
                const auto & assign = consumer_stat.assignments[num];

                assignments_topics.insertData(assign.topic_str.data(), assign.topic_str.size());

                assignments_partition_id.insert(assign.partition_id);
                assignments_current_offset.insert(assign.current_offset);
                if (assign.intent_size.has_value())
                    assignments_intent_size.insert(*assign.intent_size);
                else
                    assignments_intent_size.insertDefault();
            }
            last_assignment_num += num_assignments;

            assignments_topics_offsets.push_back(last_assignment_num);
            assignments_partition_id_offsets.push_back(last_assignment_num);
            assignments_current_offset_offsets.push_back(last_assignment_num);
            assignments_intent_size_offsets.push_back(last_assignment_num);

            for (const auto & exc : consumer_stat.exceptions_buffer)
            {
                exceptions_text.insertData(exc.text.data(), exc.text.size());
                exceptions_time.insert(exc.timestamp);
            }
            exceptions_num += consumer_stat.exceptions_buffer.size();
            exceptions_text_offset.push_back(exceptions_num);
            exceptions_time_offset.push_back(exceptions_num);


            last_poll_time.insert(consumer_stat.last_poll_time);
            num_messages_read.insert(consumer_stat.num_messages_read);
            last_commit_time.insert(consumer_stat.last_commit_timestamp);
            num_commits.insert(consumer_stat.num_commits);
            last_rebalance_time.insert(consumer_stat.last_rebalance_timestamp);

            num_rebalance_revocations.insert(consumer_stat.num_rebalance_revocations);
            num_rebalance_assignments.insert(consumer_stat.num_rebalance_assignments);

            is_currently_used.insert(consumer_stat.in_use);
            last_used.insert(static_cast<Decimal64>(consumer_stat.last_used_usec));

            rdkafka_stat.insertData(consumer_stat.rdkafka_stat.data(), consumer_stat.rdkafka_stat.size());
        }
    };

    const bool show_tables_granted = access->isGranted(AccessType::SHOW_TABLES);

    auto handle_table = [&](DatabaseTablesIteratorPtr & it, auto & kafka_table)
    {
        if (!show_tables_granted && !access->isGranted(AccessType::SHOW_TABLES, it->databaseName(), it->name()))
            return;
        add_rows(it, kafka_table);
    };
    auto databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = false});
    for (const auto & db : databases)
    {
        for (auto it = db.second->getTablesIterator(context); it->isValid(); it->next())
        {
            StoragePtr storage = it->table();
            if (auto * kafka_table = dynamic_cast<StorageKafka *>(storage.get()))
                handle_table(it, *kafka_table);
            else if (auto * kafka_2_table = dynamic_cast<StorageKafka2 *>(storage.get()))
                handle_table(it, *kafka_2_table);
        }
    }
}

}

#endif
