#include <Storages/System/StorageSystemKafkaConsumers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeUUID.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsDateTime.h>

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
        {"assignments.partition_id", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"assignments.current_offset", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
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



void StorageSystemKafkaConsumers::fillData(MutableColumns & res_columns, ContextPtr, const SelectQueryInfo &) const
{
    auto tables_mark_dropped = DatabaseCatalog::instance().getTablesMarkedDropped();

    size_t index = 0;


    auto & database = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & table = assert_cast<ColumnString &>(*res_columns[index++]);
    auto & consumer_id = assert_cast<ColumnString &>(*res_columns[index++]); //(number? or string? - single clickhouse table can have many consumers)

    auto & assigments_topics = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[index]).getData());
    auto & assigments_topics_offsets = assert_cast<ColumnArray &>(*res_columns[index++]).getOffsets();

    auto & assigments_partition_id = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[index]).getData());
    auto & assigments_partition_id_offsets = assert_cast<ColumnArray &>(*res_columns[index++]).getOffsets();

    auto & assigments_current_offset = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[index]).getData());
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


    auto add_row = [&]()
    {
        std::string fake_database = "fake_kafka_database";
        database.insertData(fake_database.data(), fake_database.size());

        std::string fake_table = "fake_kafka_table";
        table.insertData(fake_table.data(), fake_table.size());

        std::string fake_consumer_id = "fake_consumer_id";
        consumer_id.insertData(fake_consumer_id.data(), fake_consumer_id.size());

        std::string fake_assigments_topic_1 = "fake_assigments_topic_1";
        std::string fake_assigments_topic_2 = "fake_assigments_topic_2";
        assigments_topics.insertData(fake_assigments_topic_1.data(), fake_assigments_topic_1.size());
        assigments_topics.insertData(fake_assigments_topic_2.data(), fake_assigments_topic_2.size());
        assigments_topics_offsets.push_back(2);

        std::string fake_partition_id_1 = "fake_partition_id_1";
        std::string fake_partition_id_2 = "fake_partition_id_2";
        assigments_partition_id.insertData(fake_partition_id_1.data(), fake_partition_id_1.size());
        assigments_partition_id.insertData(fake_partition_id_2.data(), fake_partition_id_2.size());
        assigments_partition_id_offsets.push_back(2);

        std::string fake_current_offset_1 = "fake_current_offset_1";
        std::string fake_current_offset_2 = "fake_current_offset_2";
        assigments_current_offset.insertData(fake_current_offset_1.data(), fake_current_offset_1.size());
        assigments_current_offset.insertData(fake_current_offset_2.data(), fake_current_offset_2.size());
        assigments_current_offset_offsets.push_back(2);

        last_exception_time.insert(0);

        std::string fake_last_exception = "fake_last_exception";
        last_exception.insertData(fake_last_exception.data(), fake_last_exception.size());

        last_poll_time.insert(0);
        num_messages_read.insert(0);
        last_commit_time.insert(0);
        num_commits.insert(0);
        last_rebalance_time.insert(0);
        num_rebalance_revocations.insert(0);
        num_rebalance_assigments.insert(0);
        is_currently_used.insert(0);
    };

    add_row();
}

}
