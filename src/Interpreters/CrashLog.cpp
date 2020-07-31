#include <Interpreters/CrashLog.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Common/ClickHouseRevision.h>
#include <Common/SymbolIndex.h>

#if !defined(ARCADIA_BUILD)
#   include <Common/config_version.h>
#endif


namespace DB
{

std::weak_ptr<CrashLog> CrashLog::crash_log;


Block CrashLogElement::createBlock()
{
    return
    {
        {std::make_shared<DataTypeDate>(),                                    "event_date"},
        {std::make_shared<DataTypeDateTime>(),                                "event_time"},
        {std::make_shared<DataTypeUInt64>(),                                  "timestamp_ns"},
        {std::make_shared<DataTypeInt32>(),                                   "signal"},
        {std::make_shared<DataTypeUInt64>(),                                  "thread_id"},
        {std::make_shared<DataTypeString>(),                                  "query_id"},
        {std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()), "trace"},
        {std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "trace_full"},
        {std::make_shared<DataTypeString>(),                                  "version"},
        {std::make_shared<DataTypeUInt32>(),                                  "revision"},
        {std::make_shared<DataTypeString>(),                                  "build_id"},
    };
}

void CrashLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(DateLUT::instance().toDayNum(event_time));
    columns[i++]->insert(event_time);
    columns[i++]->insert(timestamp_ns);
    columns[i++]->insert(signal);
    columns[i++]->insert(thread_id);
    columns[i++]->insertData(query_id.data(), query_id.size());
    columns[i++]->insert(trace);
    columns[i++]->insert(trace_full);
    columns[i++]->insert(VERSION_FULL);
    columns[i++]->insert(ClickHouseRevision::get());

    String build_id_hex;
#if defined(__ELF__) && !defined(__FreeBSD__)
    build_id_hex = SymbolIndex::instance().getBuildIDHex();
#endif
    columns[i++]->insert(build_id_hex);
}

}
