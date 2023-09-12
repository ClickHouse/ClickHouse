#include "config.h"

#if USE_RDKAFKA

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeUUID.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsDateTime.h>
#include <Access/ContextAccess.h>
#include <Storages/System/StorageSystemKafkaConsumers.h>
#include <Storages/Kafka/StorageKafka.h>

#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include "base/types.h"

namespace DB
{

NamesAndTypesList StorageSystemKafkaConsumers::getNamesAndTypes()
{
    NamesAndTypesList names_and_types{
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"consumer_id", std::make_shared<DataTypeString>()}, //(number? or string? - single clickhouse table can have many consumers)
        {"assignments.topic", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"assignments.partition_id", std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>())},
        {"assignments.current_offset", std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt64>())},
        {"exceptions.time", std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>())},
        {"exceptions.text", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"last_poll_time", std::make_shared<DataTypeDateTime>()},
        {"num_messages_read", std::make_shared<DataTypeUInt64>()},
        {"last_commit_time", std::make_shared<DataTypeDateTime>()},
        {"num_commits", std::make_shared<DataTypeUInt64>()},
        {"last_rebalance_time", std::make_shared<DataTypeDateTime>()},
        {"num_rebalance_revocations", std::make_shared<DataTypeUInt64>()},
        {"num_rebalance_assignments", std::make_shared<DataTypeUInt64>()},
        {"is_currently_used", std::make_shared<DataTypeUInt8>()},
        {"rdkafka_stat", std::make_shared<DataTypeString>()},
    };
    return names_and_types;
}

void StorageSystemKafkaConsumers::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    auto tables_mark_dropped = DatabaseCatalog::instance().getTablesMarkedDropped();

    size_t index = 0;


    auto & database = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & table = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & consumer_id = assert_cast<ColumnString &>(*res_columns[index++]); //(number? or string? - single clickhouse table can have many consumers)

    auto & assigments_topics = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[index]).getData());
    auto & assigments_topics_offsets = assert_cast<ColumnArray &>(*res_columns[index++]).getOffsets();

    auto & assigments_partition_id = assert_cast<ColumnInt32 &>(assert_cast<ColumnArray &>(*res_columns[index]).getData());
    auto & assigments_partition_id_offsets = assert_cast<ColumnArray &>(*res_columns[index++]).getOffsets();

    auto & assigments_current_offset = assert_cast<ColumnInt64 &>(assert_cast<ColumnArray &>(*res_columns[index]).getData());
    auto & assigments_current_offset_offsets = assert_cast<ColumnArray &>(*res_columns[index++]).getOffsets();

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
    auto & num_rebalance_assigments = assert_cast<ColumnUInt64 &>(*res_columns[index++]);
    auto & is_currently_used = assert_cast<ColumnUInt8 &>(*res_columns[index++]);
    auto & rdkafka_stat = assert_cast<ColumnString &>(*res_columns[index++]);

    const auto access = context->getAccess();
    size_t last_assignment_num = 0;
    size_t exceptions_num = 0;

    auto add_row = [&](const DatabaseTablesIteratorPtr & it, StorageKafka * storage_kafka_ptr)
    {
        if (!access->isGranted(AccessType::SHOW_TABLES, it->databaseName(), it->name()))
        {
            return;
        }

        std::string database_str = it->databaseName();
        std::string table_str = it->name();

        auto safe_consumers = storage_kafka_ptr->getSafeConsumers();

        for (const auto & weak_consumer : safe_consumers.consumers)
        {
            if (auto consumer = weak_consumer.lock())
            {
                auto consumer_stat = consumer->getStat();

                database.insertData(database_str.data(), database_str.size());
                table.insertData(table_str.data(), table_str.size());

                consumer_id.insertData(consumer_stat.consumer_id.data(), consumer_stat.consumer_id.size());

                const auto num_assignnemts = consumer_stat.assignments.size();

                for (size_t num = 0; num < num_assignnemts; ++num)
                {
                    const auto & assign = consumer_stat.assignments[num];

                    assigments_topics.insertData(assign.topic_str.data(), assign.topic_str.size());

                    assigments_partition_id.insert(assign.partition_id);
                    assigments_current_offset.insert(assign.current_offset);
                }
                last_assignment_num += num_assignnemts;

                assigments_topics_offsets.push_back(last_assignment_num);
                assigments_partition_id_offsets.push_back(last_assignment_num);
                assigments_current_offset_offsets.push_back(last_assignment_num);

                for (const auto & exc : consumer_stat.exceptions_buffer)
                {
                    exceptions_text.insertData(exc.text.data(), exc.text.size());
                    exceptions_time.insert(exc.timestamp_usec);
                }
                exceptions_num += consumer_stat.exceptions_buffer.size();
                exceptions_text_offset.push_back(exceptions_num);
                exceptions_time_offset.push_back(exceptions_num);


                last_poll_time.insert(consumer_stat.last_poll_time);
                num_messages_read.insert(consumer_stat.num_messages_read);
                last_commit_time.insert(consumer_stat.last_commit_timestamp_usec);
                num_commits.insert(consumer_stat.num_commits);
                last_rebalance_time.insert(consumer_stat.last_rebalance_timestamp_usec);

                num_rebalance_revocations.insert(consumer_stat.num_rebalance_revocations);
                num_rebalance_assigments.insert(consumer_stat.num_rebalance_assignments);

                is_currently_used.insert(consumer_stat.in_use);

                rdkafka_stat.insertData(consumer_stat.rdkafka_stat.data(), consumer_stat.rdkafka_stat.size());
            }
        }
    };

    const bool show_tables_granted = access->isGranted(AccessType::SHOW_TABLES);

    if (show_tables_granted)
    {
        auto databases = DatabaseCatalog::instance().getDatabases();
        for (const auto & db : databases)
        {
            for (auto iterator = db.second->getTablesIterator(context); iterator->isValid(); iterator->next())
            {
                StoragePtr storage = iterator->table();
                if (auto * kafka_table = dynamic_cast<StorageKafka *>(storage.get()))
                {
                    add_row(iterator, kafka_table);
                }
            }
        }

    }
}

}

#endif
