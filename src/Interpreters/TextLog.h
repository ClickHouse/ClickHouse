#pragma once
#include <Interpreters/SystemLog.h>

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

    static std::string name() { return "TextLog"; }
    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

class TextLog : public SystemLog<TextLogElement>
{
public:
    TextLog(
        ContextPtr context_,
        const String & database_name_,
        const String & table_name_,
        const String & storage_def_,
        size_t flush_interval_milliseconds_);
};

}
