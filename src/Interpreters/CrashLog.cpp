#include <Interpreters/CrashLog.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Common/ClickHouseRevision.h>
#include <Common/SymbolIndex.h>
#include <Common/Stopwatch.h>

#include <Common/config_version.h>


namespace DB
{

std::weak_ptr<CrashLog> CrashLog::crash_log;


NamesAndTypesList CrashLogElement::getNamesAndTypes()
{
    return
    {
        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time", std::make_shared<DataTypeDateTime>()},
        {"timestamp_ns", std::make_shared<DataTypeUInt64>()},
        {"signal", std::make_shared<DataTypeInt32>()},
        {"thread_id", std::make_shared<DataTypeUInt64>()},
        {"query_id", std::make_shared<DataTypeString>()},
        {"trace", std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>())},
        {"trace_full", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"version", std::make_shared<DataTypeString>()},
        {"revision", std::make_shared<DataTypeUInt32>()},
        {"build_id", std::make_shared<DataTypeString>()},
    };
}

void CrashLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(timestamp_ns);
    columns[i++]->insert(signal);
    columns[i++]->insert(thread_id);
    columns[i++]->insertData(query_id.data(), query_id.size());
    columns[i++]->insert(trace);
    columns[i++]->insert(trace_full);
    columns[i++]->insert(VERSION_FULL);
    columns[i++]->insert(ClickHouseRevision::getVersionRevision());

    String build_id_hex;
#if defined(__ELF__) && !defined(OS_FREEBSD)
    build_id_hex = SymbolIndex::instance()->getBuildIDHex();
#endif
    columns[i++]->insert(build_id_hex);
}

}

void collectCrashLog(Int32 signal, UInt64 thread_id, const String & query_id, const StackTrace & stack_trace)
{
    using namespace DB;

    if (auto crash_log_owned = CrashLog::crash_log.lock())
    {
        UInt64 time = clock_gettime_ns(CLOCK_REALTIME);

        size_t stack_trace_size = stack_trace.getSize();
        size_t stack_trace_offset = stack_trace.getOffset();
        size_t num_frames = stack_trace_size - stack_trace_offset;

        Array trace;
        Array trace_full;

        trace.reserve(num_frames);
        trace_full.reserve(num_frames);

        for (size_t i = stack_trace_offset; i < stack_trace_size; ++i)
            trace.push_back(reinterpret_cast<uintptr_t>(stack_trace.getFramePointers()[i]));

        stack_trace.toStringEveryLine([&trace_full](const std::string & line) { trace_full.push_back(line); });

        CrashLogElement element{static_cast<time_t>(time / 1000000000), time, signal, thread_id, query_id, trace, trace_full};
        crash_log_owned->add(element);
    }
}
