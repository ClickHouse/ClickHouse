#pragma once
#include <variant>
#include <Interpreters/SystemLog.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

struct DeadLetterQueueElement
{
    enum class StreamType : int8_t
    {
        Kafka = 1,
        RabbitMQ = 2,
    };

    StreamType table_engine;
    UInt64 event_time{};
    Decimal64 event_time_microseconds{};

    String database;
    String table;
    String raw_message;
    String error;

    /// Engine specific details
    struct KafkaDetails
    {
        String topic_name;
        Int64 partition;
        Int64 offset;
        String key;
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
    std::variant<KafkaDetails, RabbitMQDetails> details;


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
