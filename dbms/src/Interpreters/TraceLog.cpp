#include <Interpreters/TraceLog.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>

namespace DB
{

Block TraceLogElement::createBlock()
{
    return
    {
        {std::make_shared<DataTypeDate>(),                                    "event_date"},
        {std::make_shared<DataTypeDateTime>(),                                "event_time"},
        {std::make_shared<DataTypeString>(),                                  "query_id"},
        {std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()), "trace"}
    };
}

void TraceLogElement::appendToBlock(Block &block) const
{
    MutableColumns columns = block.mutateColumns();

    size_t i = 0;

    columns[i++]->insert(DateLUT::instance().toDayNum(event_time));
    columns[i++]->insert(event_time);
    columns[i++]->insertData(query_id.data(), query_id.size());
    {
        Array trace_array;
        trace_array.reserve(trace.size());
        for (const UInt32 trace_address : trace)
            trace_array.emplace_back(UInt64(trace_address));
        columns[i++]->insert(trace_array);
    }

    block.setColumns(std::move(columns));
}

}
