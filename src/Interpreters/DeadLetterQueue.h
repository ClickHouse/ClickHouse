#pragma once
#include <Interpreters/SystemLog.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Storages/ColumnsDescription.h>


/// should be called ...Log for uniformity

// event_time,
// database,
// table,
// topic,
// partition,
// offset,
// raw_message,
// error

namespace DB
{




struct DeadLetterQueueElement
{
    enum class StreamType : int8_t
    {
        Kafka = 1,
        RabbitMQ = 2,
    };

    StreamType stream_type;
    UInt64 event_time{};
    Decimal64 event_time_microseconds{};

    String database_name;
    String table_name;
    String raw_message;
    String error;

    struct KafkaDetails
    {
        String topic_name;
        Int64 partition;
        Int64 offset;
    };
    struct RabbitMQDetails
    {
        String exchange_name;
        String message_id;
        UInt64 timestamp;
        bool redelivered;
        UInt64 delivery_tag;
        String channel_id;
    };
    struct Details
    {
        Details(KafkaDetails kafka_)
            :kafka(kafka_), kafka_skip_fields(0)
        {
        }
        Details(RabbitMQDetails rabbit_mq_)
            :rabbit_mq(rabbit_mq_), rabbit_mq_skip_fields(0)
        {
        }

        KafkaDetails kafka;
        size_t kafka_skip_fields = 3;

        RabbitMQDetails rabbit_mq;
        size_t rabbit_mq_skip_fields = 6;
    };
    Details details;

    static std::string name() { return "DeadLetterQueue"; }

    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases();
    void appendToBlock(MutableColumns & columns) const;
};

class DeadLetterQueue : public SystemLog<DeadLetterQueueElement>
{
    using SystemLog<DeadLetterQueueElement>::SystemLog;


};

}
