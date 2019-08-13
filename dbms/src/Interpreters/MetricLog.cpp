#include <Interpreters/MetricLog.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
<<<<<<< HEAD
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
=======
>>>>>>> 46a5ec5c16ed274956cc2051d0d2da213d975051

namespace DB
{

Block MetricLogElement::createBlock()
{
    ColumnsWithTypeAndName columns_with_type_and_name;

    columns_with_type_and_name.emplace_back(std::make_shared<DataTypeDate>(),       "event_date");
    columns_with_type_and_name.emplace_back(std::make_shared<DataTypeDateTime>(),   "event_time");

    //ProfileEvents
    for (size_t i = 0, end = ProfileEvents::end(); i < end; ++i)
    {
        std::string name;
        name += "ProfileEvent_";
        name += ProfileEvents::getName(ProfileEvents::Event(i));
        columns_with_type_and_name.emplace_back(std::make_shared<DataTypeUInt64>(), name);
    }

    //CurrentMetrics
    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
    {
        std::string name;
        name += "CurrentMetric_";
        name += CurrentMetrics::getName(ProfileEvents::Event(i));
        columns_with_type_and_name.emplace_back(std::make_shared<DataTypeInt64>(), name);
    }

    return Block(columns_with_type_and_name);
}

void MetricLogElement::appendToBlock(Block & block) const
{
    MutableColumns columns = block.mutateColumns();

    size_t iter = 0;

    columns[iter++]->insert(DateLUT::instance().toDayNum(event_time));
    columns[iter++]->insert(event_time);

    //ProfileEvents
    for (size_t i = 0, end = ProfileEvents::end(); i < end; ++i)
    {
        UInt64 value = ProfileEvents::global_counters[i];
        columns[iter++]->insert(value);
    }

    //CurrentMetrics
    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
    {
        UInt64 value = CurrentMetrics::values[i];
        columns[iter++]->insert(value);
    }
}

}
