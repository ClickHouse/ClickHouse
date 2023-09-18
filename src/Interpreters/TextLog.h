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
    UInt32 microseconds{};

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

    TextLog(
        ContextPtr context_,
        const String & database_name_,
        const String & table_name_,
        const String & storage_def_,
        size_t flush_interval_milliseconds_);

    static std::shared_ptr<Queue> getLogQueue(size_t flush_interval_milliseconds)
    {
        static std::shared_ptr<Queue> queue = std::make_shared<Queue>("text_log", flush_interval_milliseconds, true);
        return queue;
    }
};

}
