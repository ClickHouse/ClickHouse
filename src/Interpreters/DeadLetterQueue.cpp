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

        {"exchange_name", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "RabbitMQ exchange name."},
        {"message_id", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "RabbitMQ message id."},
        {"message_timestamp", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>()), "RabbitMQ message timestamp."},
        {"message_redelivered", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>()), "RabbitMQ redelivered flag."},
        {"message_delivery_tag", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "RabbitMQ delivery tag."},
        {"channel_id", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "RabbitMQ channel id."},

        {"raw_message", std::make_shared<DataTypeString>(), "Message body."}
    };
}

namespace
{
struct DetailsVisitor
{
    MutableColumns & columns;
    mutable size_t i;

    DetailsVisitor(MutableColumns & columns_, size_t i_)
        : columns(columns_), i(i_)
    {
    }

    void operator()(const DeadLetterQueueElement::KafkaDetails & kafka) const
    {
        columns[i++]->insertData(kafka.topic_name.data(), kafka.topic_name.size());
        columns[i++]->insert(kafka.partition);
        columns[i++]->insert(kafka.offset);
    }

    void operator()(const DeadLetterQueueElement::RabbitMQDetails & rabbit_mq) const
    {
        i += 3;

        columns[i++]->insertData(rabbit_mq.exchange_name.data(), rabbit_mq.exchange_name.size());
        columns[i++]->insertData(rabbit_mq.message_id.data(), rabbit_mq.message_id.size());
        columns[i++]->insert(rabbit_mq.timestamp);
        columns[i++]->insert(rabbit_mq.redelivered);
        columns[i++]->insert(rabbit_mq.delivery_tag);
        columns[i++]->insertData(rabbit_mq.channel_id.data(), rabbit_mq.channel_id.size());
    }

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

    std::visit(DetailsVisitor{columns, i}, details);

    i = 13;

    columns[i++]->insertData(raw_message.data(), raw_message.size());


}

NamesAndAliases DeadLetterQueueElement::getNamesAndAliases()
{
    return NamesAndAliases{};
}
}
