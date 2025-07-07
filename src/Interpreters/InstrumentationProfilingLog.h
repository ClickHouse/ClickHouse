#pragma once

#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/SystemLog.h>
#include <Common/QueryProfiler.h>
#include <Common/ProfileEvents.h>
#include <Common/TraceSender.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

struct InstrumentationProfilingLogElement
{
    String function_name;
    UInt64 tid{};
    time_t event_time{};
    UInt64 event_time_microseconds{};
    UInt64 duration_microseconds{};
    String query_id;
    Int32 function_id{};

    static std::string name() { return "InstrumentationProfilingLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }

    void appendToBlock(MutableColumns & columns) const;
};

class InstrumentationProfilingLog : public SystemLog<InstrumentationProfilingLogElement>
{
    using SystemLog<InstrumentationProfilingLogElement>::SystemLog;
public:
    InstrumentationProfilingLog(ContextPtr context_,
        const SystemLogSettings & settings_,
        std::shared_ptr<SystemLogQueue<InstrumentationProfilingLogElement>> queue_ = nullptr)
        : SystemLog<InstrumentationProfilingLogElement>(context_, settings_, queue_)
    {
    }
};

}
