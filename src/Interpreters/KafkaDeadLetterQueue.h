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
    };

    StreamType stream_type;
    UInt64 event_time{};
    Decimal64 event_time_microseconds{};

    String database_name;
    String table_name;
    String topic_name;
    Int64 partition;
    Int64 offset;
    String raw_message;
    String error;

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
