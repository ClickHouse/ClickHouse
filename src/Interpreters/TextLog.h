#pragma once

#include <Interpreters/SystemLog.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Poco/Message.h>

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

    static std::string name() { return "TextLog"; }
    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
    static const char * getCustomColumnList() { return nullptr; }
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
