#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/AsynchronousMetricLog.h>
#include <Interpreters/AsynchronousMetrics.h>


namespace DB
{

NamesAndTypesList AsynchronousMetricLogElement::getNamesAndTypes()
{
    return
    {
        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time", std::make_shared<DataTypeDateTime>()},
        {"metric", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"value", std::make_shared<DataTypeFloat64>(),}
    };
}

void AsynchronousMetricLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t column_idx = 0;

    columns[column_idx++]->insert(event_date);
    columns[column_idx++]->insert(event_time);
    columns[column_idx++]->insert(metric_name);
    columns[column_idx++]->insert(value);
}

void AsynchronousMetricLog::addValues(const AsynchronousMetricValues & values)
{
    AsynchronousMetricLogElement element;

    element.event_time = time(nullptr);
    element.event_date = DateLUT::instance().toDayNum(element.event_time);

    /// We will round the values to make them compress better in the table.
    /// Note: as an alternative we can also use fixed point Decimal data type,
    /// but we need to store up to UINT64_MAX sometimes.
    static constexpr double precision = 1000.0;

    for (const auto & [key, value] : values)
    {
        element.metric_name = key;
        element.value = round(value * precision) / precision;

        add(element);
    }
}

}
