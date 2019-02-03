#pragma once

#include <Interpreters/SystemLog.h>

namespace DB
{

struct TraceLogElement
{

    time_t event_time{};
    String query_id{};
    std::vector<UInt64> trace{};

    static std::string name() { return "TraceLog"; }
    static Block createBlock();
    void appendToBlock(Block & block) const;
};

class TraceLog : public SystemLog<TraceLogElement>
{
    using SystemLog<TraceLogElement>::SystemLog;
};

}
