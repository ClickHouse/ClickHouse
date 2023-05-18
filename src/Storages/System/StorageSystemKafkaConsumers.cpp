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
        // {"last_exception_time", std::make_shared<DataTypeDateTime>()},
        // {"last_exception", std::make_shared<DataTypeString>()},
        // {"last_poll_time", std::make_shared<DataTypeDateTime>()},
        // {"num_messages_read", std::make_shared<DataTypeUInt64>()},
        // {"last_commit_time", std::make_shared<DataTypeDateTime>()},
        // {"num_commits", std::make_shared<DataTypeUInt64>()},
        // {"last_rebalance_time", std::make_shared<DataTypeDateTime>()},
        // {"num_rebalance_revocations", std::make_shared<DataTypeUInt64>()},
        // {"num_rebalance_assignments", std::make_shared<DataTypeUInt64>()},
        // {"is_currently_used", std::make_shared<DataTypeUInt8>()},
    };
    return names_and_types;
}



void StorageSystemKafkaConsumers::fillData(MutableColumns & res_columns, ContextPtr, const SelectQueryInfo &) const
{

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"

    auto tables_mark_dropped = DatabaseCatalog::instance().getTablesMarkedDropped();

    size_t index = 0;

    // auto & column_index = assert_cast<ColumnUInt32 &>(*res_columns[index++]);
    // auto & column_database = assert_cast<ColumnString &>(*res_columns[index++]);
    // auto & column_table = assert_cast<ColumnString &>(*res_columns[index++]);
    // auto & column_uuid = assert_cast<ColumnUUID &>(*res_columns[index++]).getData();
    // auto & column_engine = assert_cast<ColumnString &>(*res_columns[index++]);
    // auto & column_metadata_dropped_path = assert_cast<ColumnString &>(*res_columns[index++]);
    // auto & column_table_dropped_time = assert_cast<ColumnUInt32 &>(*res_columns[index++]);


        auto & database = assert_cast<ColumnString &>(*res_columns[index++]);
        auto & table = assert_cast<ColumnString &>(*res_columns[index++]);
        auto & consumer_id = assert_cast<ColumnString &>(*res_columns[index++]); //(number? or string? - single clickhouse table can have many consumers)

        auto & assigments_topics = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[index]).getData());
        auto & assigments_topics_offsets = assert_cast<ColumnArray &>(*res_columns[index++]).getOffsets();

        auto & assigments_partition_id = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[index]).getData());
        auto & assigments_partition_id_offsets = assert_cast<ColumnArray &>(*res_columns[index++]).getOffsets();

        auto & assigments_current_offset = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[index]).getData());
        auto & assigments_current_offset_offsets = assert_cast<ColumnArray &>(*res_columns[index++]).getOffsets();

        // auto & assignments.topic = assert_cast<ColumnArray>(std::make_shared<DataTypeString>())},
        // auto & assignments.partition_id = assert_cast<ColumnArray>(std::make_shared<DataTypeString>())},
        // auto & assignments.current_offset = assert_cast<ColumnArray>(std::make_shared<DataTypeString>())},



        // auto & last_exception_time = assert_cast<ColumnDateTime &>(*res_columns[index++]);
        // auto & last_exception = assert_cast<ColumnString &>(*res_columns[index++]);
        // auto & last_poll_time = assert_cast<ColumnDateTime &>(*res_columns[index++]);
        // auto & num_messages_read = assert_cast<ColumnUInt64 &>(*res_columns[index++]);
        // auto & last_commit_time = assert_cast<ColumnDateTime &>(*res_columns[index++]);
        // auto & num_commits = assert_cast<ColumnUInt64 &>(*res_columns[index++]);
        // auto & last_rebalance_time = assert_cast<ColumnDateTime &>(*res_columns[index++]);
        // auto & num_rebalance_revocations = assert_cast<ColumnUInt64 &>(*res_columns[index++]);
        // auto & num_rebalance_assignments = assert_cast<ColumnUInt64 &>(*res_columns[index++]);
        // auto & is_currently_used = assert_cast<ColumnUInt8 &>(*res_columns[index++]);


    auto add_row = [&]()
    {
        // column_index.insertValue(idx);
        // column_database.insertData(table_mark_dropped.table_id.getDatabaseName().data(), table_mark_dropped.table_id.getDatabaseName().size());
        // column_table.insertData(table_mark_dropped.table_id.getTableName().data(), table_mark_dropped.table_id.getTableName().size());
        // column_uuid.push_back(table_mark_dropped.table_id.uuid.toUnderType());
        // if (table_mark_dropped.table)
        //     column_engine.insertData(table_mark_dropped.table->getName().data(), table_mark_dropped.table->getName().size());
        // else
        //     column_engine.insertData({}, 0);
        // column_metadata_dropped_path.insertData(table_mark_dropped.metadata_path.data(), table_mark_dropped.metadata_path.size());
        // column_table_dropped_time.insertValue(static_cast<UInt32>(table_mark_dropped.drop_time));



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


#if 0


        consumer_id.insertValue(assert_cast<ColumnString> &>(*res_columns[index++]); //(number? or string? - single clickhouse table can have many consumers)
        assignments.topic.insertValue( = assert_cast<ColumnArray>(std::make_shared<DataTypeString>())},
        assignments.partition_id.insertValue( = assert_cast<ColumnArray>(std::make_shared<DataTypeString>())},
        assignments.current_offset.insertValue( = assert_cast<ColumnArray>(std::make_shared<DataTypeString>())},
        last_exception_time.insertValue( = assert_cast<ColumnDateTime> &>(*res_columns[index++]);

        last_exception.insertValue( = assert_cast<ColumnString> &>(*res_columns[index++]);
        last_poll_time.insertValue( = assert_cast<ColumnDateTime> &>(*res_columns[index++]);
        num_messages_read.insertValue( = assert_cast<ColumnUInt64> &>(*res_columns[index++]);
        last_commit_time.insertValue( = assert_cast<ColumnDateTime> &>(*res_columns[index++]);
        num_commits.insertValue( = assert_cast<ColumnUInt63> &>(*res_columns[index++]);
last_rebalance_time = assert_cast<ColumnDateTime> &>(*res_columns[index++]);
num_rebalance_revocations = assert_cast<ColumnUInt64> &>(*res_columns[index++]);
num_rebalance_assignments = assert_cast<ColumnUInt64> &>(*res_columns[index++]);
is_currently_used = assert_cast<ColumnUInt8> &>(*res_columns[index++]);
#endif

    };

    // UInt32 idx = 0;
    // for (const auto & table_mark_dropped : tables_mark_dropped)
    //     add_row(idx++, table_mark_dropped);
    add_row();
#pragma GCC diagnostic pop
}

}
