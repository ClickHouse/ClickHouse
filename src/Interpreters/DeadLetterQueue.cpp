#include <Interpreters/DeadLetterQueue.h>

#include <Core/Settings.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>

namespace DB
{

ColumnsDescription DeadLetterQueueElement::getColumnsDescription()
{
    auto low_cardinality_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());

    auto stream_type = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values{
            {"Kafka", static_cast<Int8>(StreamType::Kafka)},
            {"RabbitMQ", static_cast<Int8>(StreamType::RabbitMQ)},
        });

    return ColumnsDescription
    {
        {"stream_type", stream_type, "Stream type. Possible values: 'Kafka', 'RabbitMQ'."},
        {"event_date", std::make_shared<DataTypeDate>(), "Message consuming date."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Message consuming date and time."},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Query starting time with microseconds precision."},
        {"database_name", low_cardinality_string, "ClickHouse database Kafka table belongs to."},
        {"table_name", low_cardinality_string, "ClickHouse table name."},
        {"error", std::make_shared<DataTypeString>(), "Error text."},
        {"topic_name", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Kafka topic name."},
        {"partition", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "Kafka partition."},
        {"offset", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "Kafka offset."},


        {"raw_message", std::make_shared<DataTypeString>(), "Message body."}
    };
}

void DeadLetterQueueElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(static_cast<Int8>(stream_type));
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);

    columns[i++]->insertData(database_name.data(), database_name.size());
    columns[i++]->insertData(table_name.data(), table_name.size());
    columns[i++]->insertData(error.data(), error.size());

    if (!details.kafka_skip_fields)
    {
        columns[i++]->insertData(details.kafka.topic_name.data(), details.kafka.topic_name.size());
        columns[i++]->insert(details.kafka.partition);
        columns[i++]->insert(details.kafka.offset);
    }
    else
        i += details.kafka_skip_fields;


    if (!details.rabbit_mq_skip_fields)
    {
        columns[i++]->insertData(details.rabbit_mq.exchange_name.data(), details.rabbit_mq.exchange_name.size());
        columns[i++]->insertData(details.rabbit_mq.message_id.data(), details.rabbit_mq.message_id.size());
        columns[i++]->insert(details.rabbit_mq.timestamp);
        columns[i++]->insert(details.rabbit_mq.redelivered);
        columns[i++]->insert(details.rabbit_mq.delivery_tag);
        columns[i++]->insertData(details.rabbit_mq.channel_id.data(), details.rabbit_mq.channel_id.size());
    }
    else
        i += details.rabbit_mq_skip_fields;


    columns[i++]->insertData(raw_message.data(), raw_message.size());


}

NamesAndAliases DeadLetterQueueElement::getNamesAndAliases()
{
    return NamesAndAliases{};
}
}
