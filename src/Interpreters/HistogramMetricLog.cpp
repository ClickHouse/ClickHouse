#include <Interpreters/HistogramMetricLog.h>

#include <base/getFQDNOrHostName.h>
#include <Common/DateLUTImpl.h>
#include <Common/HistogramMetrics.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <limits>


namespace DB
{

ColumnsDescription HistogramMetricLogElement::getColumnsDescription()
{
    ColumnsDescription result;

    result.add({"hostname", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Hostname of the server executing the query."});
    result.add({"event_date", std::make_shared<DataTypeDate>(), "Event date."});
    result.add({"event_time", std::make_shared<DataTypeDateTime>(), "Event time."});
    result.add({"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6), "Event time with microseconds resolution."});
    result.add({"metric", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "Metric name."});
    result.add({"labels",
                std::make_shared<DataTypeMap>(
                    std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
                    std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())),
                "Metric labels."});
    result.add({"histogram",
                std::make_shared<DataTypeMap>(std::make_shared<DataTypeFloat64>(), std::make_shared<DataTypeUInt64>()),
                "Cumulative histogram: maps bucket upper bound to number of observations ≤ that bound; includes +inf as the final bucket."});
    result.add({"count", std::make_shared<DataTypeUInt64>(), "Total number of observations, equals histogram[+inf]."});
    result.add({"sum", std::make_shared<DataTypeFloat64>(), "Sum of all observed values."});

    return result;
}

void HistogramMetricLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t column_idx = 0;

    columns[column_idx++]->insert(getFQDNOrHostName());
    columns[column_idx++]->insert(event_date);
    columns[column_idx++]->insert(event_time);
    columns[column_idx++]->insert(event_time_microseconds);
    columns[column_idx++]->insert(metric_name);
    columns[column_idx++]->insert(labels);
    columns[column_idx++]->insert(histogram);
    columns[column_idx++]->insert(count);
    columns[column_idx++]->insert(sum);
}

void HistogramMetricLog::stepFunction(TimePoint current_time)
{
    const auto event_time = std::chrono::system_clock::to_time_t(current_time);
    const auto event_date = static_cast<UInt16>(DateLUT::instance().toDayNum(event_time));
    const auto event_time_microseconds = timeInMicroseconds(current_time);

    const auto & factory = HistogramMetrics::Factory::instance();
    factory.forEachFamily([&](const HistogramMetrics::MetricFamily & family)
    {
        const auto & buckets = family.getBuckets();
        const auto & label_names = family.getLabels();
        const auto & metric_name = family.getName();

        family.forEachMetric([&](const HistogramMetrics::LabelValues & label_values, const HistogramMetrics::Metric & metric)
        {
            HistogramMetricLogElement elem;
            elem.event_time = event_time;
            elem.event_date = event_date;
            elem.event_time_microseconds = event_time_microseconds;
            elem.metric_name = metric_name;

            elem.labels.reserve(label_values.size());
            for (size_t i = 0; i < label_values.size(); ++i)
            {
                elem.labels.push_back(Tuple{label_names[i], label_values[i]});
            }

            elem.histogram.reserve(buckets.size() + 1);
            UInt64 cumulative = 0;
            for (size_t i = 0; i < buckets.size() + 1; ++i)
            {
                cumulative += metric.getCounter(i);
                Float64 bound = (i < buckets.size()) ? buckets[i] : std::numeric_limits<Float64>::infinity();
                elem.histogram.push_back(Tuple{bound, cumulative});
            }

            elem.count = cumulative;
            elem.sum = metric.getSum();

            add(std::move(elem));
        });
    });
}

}
