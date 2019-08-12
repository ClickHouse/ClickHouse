#include <Interpreters/MetricLog.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>

namespace DB
{

Block MetricLogElement::createBlock()
{
    ColumnsWithTypeAndName columns_with_type_and_name;
    //ProfileEvents
    for (size_t i = 0, end = ProfileEvents::end(); i < end; ++i)
    {
        UInt64 value = ProfileEvents::global_counters[i];

        if (0 != value)
        {
            std::string name;
            name += "ProfileEvent_";
            name += ProfileEvents::getName(ProfileEvents::Event(i));
            columns_with_type_and_name.emplace_back(std::make_shared<DataTypeUInt64>(), name);
        }
    }

    //CurrentMetrics
    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
    {
        UInt64 value = CurrentMetrics::values[i];

        if (0 != value)
        {
            std::string name;
            name += "CurrentMetric_";
            name += CurrentMetrics::getName(ProfileEvents::Event(i));
            columns_with_type_and_name.emplace_back(std::make_shared<DataTypeInt64>(), name);
        }
    }

    return Block(columns_with_type_and_name);
}

void MetricLogElement::appendToBlock(Block & block) const
{
    MutableColumns columns = block.mutateColumns();

    size_t iter = 0;

    //ProfileEvents
    for (size_t i = 0, end = ProfileEvents::end(); i < end; ++i)
    {
        UInt64 value = ProfileEvents::global_counters[i];

        if (0 != value)
        {
            columns[iter++]->insert(value);
        }
    }

    //CurrentMetrics
    for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
    {
        UInt64 value = CurrentMetrics::values[i];

        if (0 != value)
        {
            columns[iter++]->insert(value);
        }
    }
}

}
