#include <Interpreters/DeadLetterQueue.h>

#include <Core/Settings.h>
#include <Common/DateLUTImpl.h>
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

    auto table_engine = std::make_shared<DataTypeEnum8>(
        DataTypeEnum8::Values{
            {"Kafka", static_cast<Int8>(StreamType::Kafka)},
            {"RabbitMQ", static_cast<Int8>(StreamType::RabbitMQ)},
        });

    return ColumnsDescription
    {
        {"table_engine", table_engine, "Stream type. Possible values: 'Kafka', 'RabbitMQ'."},
        {"event_date", std::make_shared<DataTypeDate>(), "Message consuming date."},
        {"event_time", std::make_shared<DataTypeDateTime>(), "Message consuming date and time."},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Query starting time with microseconds precision."},
        {"database", low_cardinality_string, "ClickHouse database Kafka table belongs to."},
        {"table", low_cardinality_string, "ClickHouse table name."},
        {"error", std::make_shared<DataTypeString>(), "Error text."},
        {"raw_message", std::make_shared<DataTypeString>(), "Message body."},

        {"kafka_topic_name", std::make_shared<DataTypeString>(), "Kafka topic name."},
        {"kafka_partition", std::make_shared<DataTypeUInt64>(), "Kafka partition of the topic."},
        {"kafka_offset", std::make_shared<DataTypeUInt64>(), "Kafka offset of the message."},
        {"kafka_key", std::make_shared<DataTypeString>(), "Kafka key of the message."},
        /* 4 */

        {"rabbitmq_exchange_name", std::make_shared<DataTypeString>(), "RabbitMQ exchange name."},
        {"rabbitmq_message_id", std::make_shared<DataTypeString>(), "RabbitMQ message id."},
        {"rabbitmq_message_timestamp", std::make_shared<DataTypeDateTime>(), "RabbitMQ message timestamp."},
        {"rabbitmq_message_redelivered", std::make_shared<DataTypeUInt8>(), "RabbitMQ redelivered flag."},
        {"rabbitmq_message_delivery_tag", std::make_shared<DataTypeUInt64>(), "RabbitMQ delivery tag."},
        {"rabbitmq_channel_id", std::make_shared<DataTypeString>(), "RabbitMQ channel id."},
        /* 10 */

    };
}

namespace
{
struct DetailsVisitor
{
    MutableColumns & columns;
    mutable size_t i;
    std::vector<bool> & explicitly_set;

    DetailsVisitor(MutableColumns & columns_, size_t i_, std::vector<bool> & explicitly_set_)
        : columns(columns_), i(i_), explicitly_set(explicitly_set_)
    {
    }

    void operator()(const DeadLetterQueueElement::KafkaDetails & kafka) const
    {
        explicitly_set[i] = true;
        columns[i++]->insertData(kafka.topic_name.data(), kafka.topic_name.size());
        explicitly_set[i] = true;
        columns[i++]->insert(kafka.partition);
        explicitly_set[i] = true;
        columns[i++]->insert(kafka.offset);
        explicitly_set[i] = true;
        columns[i++]->insertData(kafka.key.data(), kafka.key.size());
    }

    void operator()(const DeadLetterQueueElement::RabbitMQDetails & rabbit_mq) const
    {
        i += 4; // skip rows specific for previous details

        explicitly_set[i] = true;
        columns[i++]->insertData(rabbit_mq.exchange_name.data(), rabbit_mq.exchange_name.size());
        explicitly_set[i] = true;
        columns[i++]->insertData(rabbit_mq.message_id.data(), rabbit_mq.message_id.size());
        explicitly_set[i] = true;
        columns[i++]->insert(rabbit_mq.timestamp);
        explicitly_set[i] = true;
        columns[i++]->insert(rabbit_mq.redelivered);
        explicitly_set[i] = true;
        columns[i++]->insert(rabbit_mq.delivery_tag);
        explicitly_set[i] = true;
        columns[i++]->insertData(rabbit_mq.channel_id.data(), rabbit_mq.channel_id.size());
    }
};
}

void DeadLetterQueueElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(static_cast<Int8>(table_engine));
    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);

    columns[i++]->insertData(database.data(), database.size());
    columns[i++]->insertData(table.data(), table.size());
    columns[i++]->insertData(error.data(), error.size());

    columns[i++]->insertData(raw_message.data(), raw_message.size());

    auto columns_description = getColumnsDescription();
    size_t num_columns = columns_description.size();
    std::vector<bool> explicitly_set(num_columns, false);

    std::visit(DetailsVisitor{columns, i, explicitly_set}, details);

    auto it = explicitly_set.begin();
    it += i; // start from the beginning of details

    for (; it != explicitly_set.end(); ++it)
    {
        if (! *it)
            // not set by a details visitor
            columns[i]->insertDefault();
        i++;
    }
}

NamesAndAliases DeadLetterQueueElement::getNamesAndAliases()
{
    return NamesAndAliases{};
}

}
