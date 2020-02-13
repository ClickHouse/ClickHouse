#include <Interpreters/TraceLog.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Common/ClickHouseRevision.h>


using namespace DB;

using TraceDataType = TraceLogElement::TraceDataType;

const TraceDataType::Values TraceLogElement::trace_values = {
    {"Real", static_cast<UInt8>(TraceType::REAL_TIME)},
    {"CPU", static_cast<UInt8>(TraceType::CPU_TIME)},
    {"Memory", static_cast<UInt8>(TraceType::MEMORY)},
};

Block TraceLogElement::createBlock()
{
    return
    {
        {std::make_shared<DataTypeDate>(),                                    "event_date"},
        {std::make_shared<DataTypeDateTime>(),                                "event_time"},
        {std::make_shared<DataTypeUInt32>(),                                  "revision"},
        {std::make_shared<TraceDataType>(trace_values),                       "trace_type"},
        {std::make_shared<DataTypeUInt64>(),                                  "thread_id"},
        {std::make_shared<DataTypeString>(),                                  "query_id"},
        {std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()), "trace"},
        {std::make_shared<DataTypeUInt64>(),                                  "size"},
    };
}

void TraceLogElement::appendToBlock(Block & block) const
{
    MutableColumns columns = block.mutateColumns();

    size_t i = 0;

    columns[i++]->insert(DateLUT::instance().toDayNum(event_time));
    columns[i++]->insert(event_time);
    columns[i++]->insert(ClickHouseRevision::get());
    columns[i++]->insert(static_cast<UInt8>(trace_type));
    columns[i++]->insert(thread_id);
    columns[i++]->insertData(query_id.data(), query_id.size());
    columns[i++]->insert(trace);
    columns[i++]->insert(size);

    block.setColumns(std::move(columns));
}
