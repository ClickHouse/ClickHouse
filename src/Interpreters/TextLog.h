#pragma once

#include <Interpreters/SystemLog.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Poco/Message.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

using Poco::Message;

struct TextLogElement
{
    time_t event_time{};
    Decimal64 event_time_microseconds{};

    String thread_name;
    UInt64 thread_id{};

    Message::Priority level = Message::PRIO_TRACE;

    String query_id;
    String logger_name;
    String message;

    String source_file;
    UInt64 source_line{};

    std::string_view message_format_string;
    String value1;
    String value2;
    String value3;
    String value4;
    String value5;
    String value6;
    String value7;
    String value8;
    String value9;
    String value10;

    static std::string name() { return "TextLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

class TextLog : public SystemLog<TextLogElement>
{
public:
    using Queue = SystemLogQueue<TextLogElement>;

    explicit TextLog(ContextPtr context_, const SystemLogSettings & settings);

    static std::shared_ptr<Queue> getLogQueue(const SystemLogQueueSettings & settings)
    {
        static std::shared_ptr<Queue> queue = std::make_shared<Queue>(settings);
        return queue;
    }

    static consteval bool shouldTurnOffLogger() { return true; }
};

}
