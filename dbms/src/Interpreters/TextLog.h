#pragma once
#include <Interpreters/SystemLog.h>

namespace DB
{

using Poco::Message;

struct TextLogElement
{
    time_t event_time{};
    UInt32 microseconds;

    String thread_name;
    UInt32 os_thread_id;
    UInt32 thread_number;

    Message::Priority level = Message::PRIO_TRACE;

    String query_id;
    String logger_name;
    String message;

    String source_file;
    UInt64 source_line;

    static std::string name() { return "TextLog"; }
    static Block createBlock();
    void appendToBlock(Block & block) const;
};

class TextLog : public SystemLog<TextLogElement>
{
    using SystemLog<TextLogElement>::SystemLog;
};

}
