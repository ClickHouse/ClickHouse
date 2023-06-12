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
        {"last_exception_time", std::make_shared<DataTypeDateTime>()},
        {"last_exception", std::make_shared<DataTypeString>()},
        {"last_poll_time", std::make_shared<DataTypeDateTime>()},
        {"num_messages_read", std::make_shared<DataTypeUInt64>()},
        {"last_commit_time", std::make_shared<DataTypeDateTime>()},
        {"num_commits", std::make_shared<DataTypeUInt64>()},
        {"last_rebalance_time", std::make_shared<DataTypeDateTime>()},
        {"num_rebalance_revocations", std::make_shared<DataTypeUInt64>()},
        {"num_rebalance_assignments", std::make_shared<DataTypeUInt64>()},
        {"is_currently_used", std::make_shared<DataTypeUInt8>()},
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


    auto & last_exception_time = assert_cast<ColumnDateTime &>(*res_columns[index++]);
    auto & last_exception = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & last_poll_time = assert_cast<ColumnDateTime &>(*res_columns[index++]);
    auto & num_messages_read = assert_cast<ColumnUInt64 &>(*res_columns[index++]);
    auto & last_commit_time = assert_cast<ColumnDateTime &>(*res_columns[index++]);
    auto & num_commits = assert_cast<ColumnUInt64 &>(*res_columns[index++]);
    auto & last_rebalance_time = assert_cast<ColumnDateTime &>(*res_columns[index++]);
    auto & num_rebalance_revocations = assert_cast<ColumnUInt64 &>(*res_columns[index++]);
    auto & num_rebalance_assigments = assert_cast<ColumnUInt64 &>(*res_columns[index++]);
    auto & is_currently_used = assert_cast<ColumnUInt8 &>(*res_columns[index++]);

    const auto access = context->getAccess();
    size_t last_assignment_num = 0;

    auto add_row = [&](const DatabaseTablesIteratorPtr & it, StorageKafka * storage_kafka_ptr)
    {
        if (!access->isGranted(AccessType::SHOW_TABLES, it->databaseName(), it->name()))
        {
            return;
        }

        std::string database_str = it->databaseName();
        std::string table_str = it->name();

        for (auto consumer : storage_kafka_ptr->consumers)
        {
            auto & cpp_consumer = consumer->consumer;

            database.insertData(database_str.data(), database_str.size());
            table.insertData(table_str.data(), table_str.size());

            std::string consumer_id_str = cpp_consumer->get_member_id();
            consumer_id.insertData(consumer_id_str.data(), consumer_id_str.size());

            if (consumer->assignment.has_value() && consumer->assignment.value().size() > 0)
            {
                for (const auto & assignment : consumer->assignment.value())
                {
                    const auto & topic_str = assignment.get_topic();
                    assigments_topics.insertData(topic_str.data(), topic_str.size());

                    assigments_partition_id.insert(assignment.get_partition());
                    assigments_current_offset.insert(assignment.get_offset());
                }
                last_assignment_num += consumer->assignment.value().size();

            }
            else
            {
                std::string fake_assigments_topic = "no assigned topic";
                assigments_topics.insertData(fake_assigments_topic.data(), fake_assigments_topic.size());

                assigments_partition_id.insert(0);
                assigments_current_offset.insert(0);

                last_assignment_num += 1;
            }
            assigments_topics_offsets.push_back(last_assignment_num);
            assigments_partition_id_offsets.push_back(last_assignment_num);
            assigments_current_offset_offsets.push_back(last_assignment_num);



            // std::string fake_assigments_topic_1 = "fake_assigments_topic_1";
            // std::string fake_assigments_topic_2 = "fake_assigments_topic_2";
            // assigments_topics.insertData(fake_assigments_topic_1.data(), fake_assigments_topic_1.size());
            // assigments_topics.insertData(fake_assigments_topic_2.data(), fake_assigments_topic_2.size());
            // assigments_topics_last_offset += 2;
            // assigments_topics_offsets.push_back(assigments_topics_last_offset);

            // std::string fake_partition_id_1 = "fake_partition_id_1";
            // std::string fake_partition_id_2 = "fake_partition_id_2";
            // assigments_partition_id.insertData(fake_partition_id_1.data(), fake_partition_id_1.size());
            // assigments_partition_id.insertData(fake_partition_id_2.data(), fake_partition_id_2.size());
            // assigments_partition_id_last_offset += 2;
            // assigments_partition_id_offsets.push_back(assigments_partition_id_last_offset);

            // std::string fake_current_offset_1 = "fake_current_offset_1";
            // std::string fake_current_offset_2 = "fake_current_offset_2";
            // assigments_current_offset.insertData(fake_current_offset_1.data(), fake_current_offset_1.size());
            // assigments_current_offset.insertData(fake_current_offset_2.data(), fake_current_offset_2.size());
            // assigments_current_offset_last_offset += 2;
            // assigments_current_offset_offsets.push_back(assigments_current_offset_last_offset);

            auto exception_info = consumer->getExceptionInfo();


            last_exception.insertData(exception_info.first.data(), exception_info.first.size());
            last_exception_time.insert(exception_info.second);

            last_poll_time.insert(consumer->last_poll_timestamp_usec.load());
            num_messages_read.insert(consumer->num_messages_read.load());
            last_commit_time.insert(consumer->last_commit_timestamp_usec.load());
            num_commits.insert(consumer->num_commits.load());
            last_rebalance_time.insert(consumer->last_rebalance_timestamp_usec.load());

            num_rebalance_revocations.insert(consumer->num_rebalance_revocations.load());
            num_rebalance_assigments.insert(consumer->num_rebalance_assignments.load());

            is_currently_used.insert(consumer->stalled_status != KafkaConsumer::CONSUMER_STOPPED && consumer->assignment.has_value() && consumer->assignment.value().size() > 0);
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
