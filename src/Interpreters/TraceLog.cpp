#include <Interpreters/TraceLog.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Common/ClickHouseRevision.h>


namespace DB
{

using TraceDataType = TraceLogElement::TraceDataType;

const TraceDataType::Values TraceLogElement::trace_values =
{
    {"Real", static_cast<UInt8>(TraceType::Real)},
    {"CPU", static_cast<UInt8>(TraceType::CPU)},
    {"Memory", static_cast<UInt8>(TraceType::Memory)},
    {"MemorySample", static_cast<UInt8>(TraceType::MemorySample)},
};

Block TraceLogElement::createBlock()
{
    return
    {
        {std::make_shared<DataTypeDate>(),                                    "event_date"},
        {std::make_shared<DataTypeDateTime>(),                                "event_time"},
        {std::make_shared<DataTypeUInt64>(),                                  "timestamp_ns"},
        {std::make_shared<DataTypeUInt32>(),                                  "revision"},
        {std::make_shared<TraceDataType>(trace_values),                       "trace_type"},
        {std::make_shared<DataTypeUInt64>(),                                  "thread_id"},
        {std::make_shared<DataTypeString>(),                                  "query_id"},
        {std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()), "trace"},
        {std::make_shared<DataTypeInt64>(),                                   "size"},
    };
}

void TraceLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(DateLUT::instance().toDayNum(event_time));
    columns[i++]->insert(event_time);
    columns[i++]->insert(timestamp_ns);
    columns[i++]->insert(ClickHouseRevision::get());
    columns[i++]->insert(static_cast<UInt8>(trace_type));
    columns[i++]->insert(thread_id);
    columns[i++]->insertData(query_id.data(), query_id.size());
    columns[i++]->insert(trace);
    columns[i++]->insert(size);
}

}
