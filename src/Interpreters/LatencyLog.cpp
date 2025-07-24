#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/LatencyLog.h>
#include <base/getFQDNOrHostName.h>
#include <Common/DateLUT.h>
#include <Common/LatencyBuckets.h>
#include <Common/DateLUTImpl.h>
#include <DataTypes/DataTypeArray.h>
#include <Common/ThreadPool.h>
#include <Columns/IColumn.h>
#include <Storages/ColumnsDescription.h>


namespace DB
{

ColumnsDescription LatencyLogElement::getColumnsDescription()
{
    ColumnsDescription result;

    result.add({"hostname", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Hostname of the server executing the query."});
    result.add({"event_date", std::make_shared<DataTypeDate>(), "Event date."});
    result.add({"event_time", std::make_shared<DataTypeDateTime>(), "Event time."});
    result.add({"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Event time with microseconds resolution."});

    for (size_t i = 0, end = LatencyBuckets::end(); i < end; ++i)
    {
        auto name = fmt::format("LatencyEvent_{}", LatencyBuckets::getName(LatencyBuckets::LatencyEvent(i)));
        const auto * comment = LatencyBuckets::getDocumentation(LatencyBuckets::LatencyEvent(i));
        result.add({std::move(name), std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()), comment});
    }

    return result;
}

void LatencyLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t column_idx = 0;

    columns[column_idx++]->insert(getFQDNOrHostName());
    columns[column_idx++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[column_idx++]->insert(event_time);
    columns[column_idx++]->insert(event_time_microseconds);

    for (size_t i = 0, end = LatencyBuckets::end(); i < end; ++i)
        columns[column_idx++]->insert(Array(latency_buckets_values[i].begin(), latency_buckets_values[i].end()));
}

void LatencyLog::stepFunction(const std::chrono::system_clock::time_point current_time)
{
    LatencyLogElement elem;
    elem.event_time = std::chrono::system_clock::to_time_t(current_time);
    elem.event_time_microseconds = timeInMicroseconds(current_time);

    elem.latency_buckets_values.resize(LatencyBuckets::end());
    for (LatencyBuckets::LatencyEvent i = LatencyBuckets::LatencyEvent(0), end = LatencyBuckets::end(); i < end; ++i)
    {
        LatencyBuckets::Count sum = 0;
        for (auto & bucket : LatencyBuckets::global_bucket_lists[i])
        {
            sum += bucket.load(std::memory_order_relaxed);
            elem.latency_buckets_values[i].emplace_back(sum);
        }
    }

    this->add(std::move(elem));
}

}
