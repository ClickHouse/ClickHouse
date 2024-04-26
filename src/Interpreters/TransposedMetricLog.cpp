
#include <Interpreters/TransposedMetricLog.h>


#include <base/getFQDNOrHostName.h>
#include <Common/DateLUTImpl.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadPool.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>


namespace DB
{

ColumnsDescription TransposedMetricLogElement::getColumnsDescription()
{
    ParserCodec codec_parser;
    return ColumnsDescription
    {
        {
            "hostname",
            std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Hostname of the server executing the query."
        },
        {
            "event_date",
            std::make_shared<DataTypeDate>(),
            parseQuery(codec_parser, "(Delta(2), ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Event date."
        },
        {
            "event_time",
            std::make_shared<DataTypeDateTime>(),
            parseQuery(codec_parser, "(Delta(4), ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Event time."
        },
        {
            "metric",
            std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
            parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Metric name."
        },
        {
            "value",
            std::make_shared<DataTypeInt64>(),
            parseQuery(codec_parser, "(ZSTD(3))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
            "Metric value."
        }
    };
}


void TransposedMetricLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t column_idx = 0;

    columns[column_idx++]->insert(getFQDNOrHostName());
    columns[column_idx++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[column_idx++]->insert(event_time);
    columns[column_idx++]->insert(metric_name);
    columns[column_idx++]->insert(value);
}


void TransposedMetricLog::startCollectMetric(size_t collect_interval_milliseconds_)
{
    collect_interval_milliseconds = collect_interval_milliseconds_;
    is_shutdown_metric_thread = false;
    metric_flush_thread = std::make_unique<ThreadFromGlobalPool>([this] { metricThreadFunction(); });
}


void TransposedMetricLog::stopCollectMetric()
{
    bool old_val = false;
    if (!is_shutdown_metric_thread.compare_exchange_strong(old_val, true))
        return;
    if (metric_flush_thread)
        metric_flush_thread->join();
}


void TransposedMetricLog::shutdown()
{
    stopCollectMetric();
    stopFlushThread();
}


void TransposedMetricLog::metricThreadFunction()
{
    auto desired_timepoint = std::chrono::system_clock::now();

    /// For differentiation of ProfileEvents counters.
    std::vector<ProfileEvents::Count> prev_profile_events(ProfileEvents::end());

    while (!is_shutdown_metric_thread)
    {
        try
        {
            const auto current_time = std::chrono::system_clock::now();

            TransposedMetricLogElement elem;
            elem.event_time = std::chrono::system_clock::to_time_t(current_time);

            for (ProfileEvents::Event i = ProfileEvents::Event(0), end = ProfileEvents::end(); i < end; ++i)
            {
                const ProfileEvents::Count new_value = ProfileEvents::global_counters[i].load(std::memory_order_relaxed);
                auto & old_value = prev_profile_events[i];
                elem.metric_name = fmt::format("ProfileEvent_{}", ProfileEvents::getName(ProfileEvents::Event(i)));
                elem.value = new_value - old_value;
                old_value = new_value;
                this->add(elem);
            }

            for (size_t i = 0, end = CurrentMetrics::end(); i < end; ++i)
            {
                elem.metric_name = fmt::format("CurrentMetric_{}", CurrentMetrics::getName(CurrentMetrics::Metric(i)));
                elem.value = CurrentMetrics::values[i];
                this->add(elem);
            }

            /// We will record current time into table but align it to regular time intervals to avoid time drift.
            /// We may drop some time points if the server is overloaded and recording took too much time.
            while (desired_timepoint <= current_time)
                desired_timepoint += std::chrono::milliseconds(collect_interval_milliseconds);

            std::this_thread::sleep_until(desired_timepoint);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

}
