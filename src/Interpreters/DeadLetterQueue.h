#pragma once
#include <variant>
#include <Interpreters/SystemLog.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

struct DeadLetterElement
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


    static std::string name() { return "DeadLetter"; }

    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases();
    void appendToBlock(MutableColumns & columns) const;
};

class DeadLetter : public SystemLog<DeadLetterElement>
{
    using SystemLog<DeadLetterElement>::SystemLog;
};

}
