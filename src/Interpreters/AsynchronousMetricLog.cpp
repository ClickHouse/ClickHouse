#include <Interpreters/AsynchronousMetricLog.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/AsynchronousMetrics.h>


namespace DB
{

Block AsynchronousMetricLogElement::createBlock()
{
    ColumnsWithTypeAndName columns;

    columns.emplace_back(std::make_shared<DataTypeDate>(),          "event_date");
    columns.emplace_back(std::make_shared<DataTypeDateTime>(),      "event_time");
    columns.emplace_back(std::make_shared<DataTypeDateTime64>(6),   "event_time_microseconds");
    columns.emplace_back(std::make_shared<DataTypeString>(),        "name");
    columns.emplace_back(std::make_shared<DataTypeFloat64>(),       "value");

    return Block(columns);
}


void AsynchronousMetricLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t column_idx = 0;

    columns[column_idx++]->insert(event_date);
    columns[column_idx++]->insert(event_time);
    columns[column_idx++]->insert(event_time_microseconds);
    columns[column_idx++]->insert(metric_name);
    columns[column_idx++]->insert(value);
}


inline UInt64 time_in_milliseconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(timepoint.time_since_epoch()).count();
}

inline UInt64 time_in_microseconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::microseconds>(timepoint.time_since_epoch()).count();
}


inline UInt64 time_in_seconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::seconds>(timepoint.time_since_epoch()).count();
}

void AsynchronousMetricLog::addValues(const AsynchronousMetricValues & values)
{
    AsynchronousMetricLogElement element;

    const auto now = std::chrono::system_clock::now();
    element.event_time = time_in_seconds(now);
    element.event_time_microseconds = time_in_microseconds(now);
    element.event_date = DateLUT::instance().toDayNum(element.event_time);

    for (const auto & [key, value] : values)
    {
        element.metric_name = key;
        element.value = value;

        add(element);
    }
}

}
