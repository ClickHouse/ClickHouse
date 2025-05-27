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

struct XRayInstrumentationProfilingLogElement
{
    String function_name;
    UInt64 tid{};
    time_t event_time{};
    UInt64 event_time_microseconds{};
    UInt64 duration_microseconds{};
    String query_id;
    Int32 function_id{};

    static std::string name() { return "XRayInstrumentationProfilingLog"; }
    static ColumnsDescription getColumnsDescription();
    static NamesAndAliases getNamesAndAliases() { return {}; }

    void appendToBlock(MutableColumns & columns) const;
};

class XRayInstrumentationProfilingLog : public SystemLog<XRayInstrumentationProfilingLogElement>
{
    using SystemLog<XRayInstrumentationProfilingLogElement>::SystemLog;
public:
    XRayInstrumentationProfilingLog(ContextPtr context_,
        const SystemLogSettings & settings_,
        std::shared_ptr<SystemLogQueue<XRayInstrumentationProfilingLogElement>> queue_ = nullptr)
        : SystemLog<XRayInstrumentationProfilingLogElement>(context_, settings_, queue_)
    {
    }
};

}
