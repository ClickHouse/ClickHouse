#pragma once

#include <Core/NamesAndAliases.h>
#include <Core/NamesAndTypes.h>
#include <base/types.h>

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

    static std::string name() { return "TextLog"; }
    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
};

}
