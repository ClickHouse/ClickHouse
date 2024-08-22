#include <Interpreters/KafkaDeadLetterQueue.h>

#include <Core/Settings.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>


namespace DB
{

ColumnsDescription DeadLetterQueueElement::getColumnsDescription()
{
    auto low_cardinality_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());

    return ColumnsDescription
    {
        {"event_date", std::make_shared<DataTypeDate>(), "Message consuming date."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Message consuming time."},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Query starting time with microseconds precision."},
        {"database_name", low_cardinality_string, "ClickHouse database Kafka table belongs to."},
        {"table_name", low_cardinality_string, "ClickHouse table name."},
        {"topic_name", low_cardinality_string, "Topic name."},
        {"partition", std::make_shared<DataTypeUInt64>(), "Partition."},
        {"offset", std::make_shared<DataTypeUInt64>(), "Offset."},
        {"raw_message", std::make_shared<DataTypeString>(), "Message body."},
        {"error", std::make_shared<DataTypeString>(), "Error text."}
    };
}

void DeadLetterQueueElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);

    columns[i++]->insertData(database_name.data(), database_name.size());
    columns[i++]->insertData(table_name.data(), table_name.size());
    columns[i++]->insertData(topic_name.data(), topic_name.size());

    columns[i++]->insert(partition);
    columns[i++]->insert(offset);

    columns[i++]->insertData(raw_message.data(), raw_message.size());
    columns[i++]->insertData(error.data(), error.size());


}

NamesAndAliases DeadLetterQueueElement::getNamesAndAliases()
{
    return NamesAndAliases{};
}
}
